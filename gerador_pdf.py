# gerador_pdf.py (CORREÇÃO v8 - Corrigido o 'dt.tz_convert')

import io
from fpdf import FPDF, Align
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
import json
import threading
import time
import traceback

import data_source
import processamento
from config import PONTOS_DE_ANALISE, RISCO_MAP, STATUS_MAP_HIERARQUICO
import matplotlib

matplotlib.use('Agg')

EXCEL_CACHE = {}
EXCEL_CACHE_LOCK = threading.Lock()
PDF_CACHE = {}
PDF_CACHE_LOCK = threading.Lock()


def criar_relatorio_em_memoria(df_dados, fig_chuva_mp, fig_umidade_mp, status_texto, status_cor, periodo_str=""):
    # ... (código idêntico, sem alterações) ...
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Helvetica", size=10)
    pdf.set_font("Helvetica", "B", 14)
    id_ponto_rel = df_dados.iloc[0]['id_ponto'] if not df_dados.empty else "Monitoramento"
    pdf.cell(0, 10, f"Relatório de Monitoramento - {id_ponto_rel}", ln=True, align="C")
    data_inicio_local = df_dados['timestamp_local'].min().strftime('%d/%m/%Y %H:%M')
    data_fim_local = df_dados['timestamp_local'].max().strftime('%d/%m/%Y %H:%M')
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 5, f"Período: {data_inicio_local} a {data_fim_local}", ln=True, align="C")
    pdf.cell(0, 5, f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}", ln=True, align="C")
    pdf.ln(5)
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(pdf.w / 2 - pdf.l_margin, 8, "Status Geral do Período:", border=1, align="L")
    cor_fundo = (200, 200, 200)
    if status_cor == 'success':
        cor_fundo = (180, 255, 180)
    elif status_cor == 'warning':
        cor_fundo = (255, 255, 150)
    elif status_cor == 'danger':
        cor_fundo = (255, 180, 180)
    pdf.set_fill_color(*cor_fundo)
    pdf.cell(0, 8, status_texto, border=1, ln=True, align="C", fill=True)
    pdf.ln(5)

    def _add_matplotlib_fig(fig, base_title, periodo_str):
        full_title = f"{base_title} {periodo_str}"
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(0, 5, full_title, ln=True, align="L")
        try:
            img_bytes = io.BytesIO()
            fig.savefig(img_bytes, format="png", bbox_inches="tight")
            img_bytes.seek(0)
            plt.close(fig)
            pdf.image(img_bytes, x=pdf.l_margin, y=None, w=pdf.w - 2 * pdf.l_margin)
            pdf.ln(5)
        except Exception as e:
            pdf.set_font("Helvetica", "I", 10)
            pdf.cell(0, 5, f"AVISO: Não foi possível gerar o gráfico de {base_title}. Erro: {e}", ln=True, align="C")
            pdf.ln(5)

    _add_matplotlib_fig(fig_chuva_mp, "Pluviometria", periodo_str)
    _add_matplotlib_fig(fig_umidade_mp, "Umidade do Solo", periodo_str)
    pdf.add_page()
    pdf.ln(5)
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 10, "Últimos 30 Registros do Período", ln=True, align="L")
    df_dados.loc[:, 'timestamp_local_str'] = df_dados['timestamp_local'].apply(
        lambda x: x.strftime('%d/%m/%Y %H:%M:%S') if pd.notna(x) else '-')
    df_ultimos = df_dados[
        ['timestamp_local_str', 'chuva_mm', 'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc']].tail(30).copy()
    col_widths = [45, 35, 30, 30, 30]
    headers = ["Data/Hora", "Chuva (mm/h)", "Umidade 1m (%)", "Umidade 2m (%)", "Umidade 3m (%)"]
    pdf.set_font("Helvetica", "B", 9)
    for w, h in zip(col_widths, headers):
        pdf.cell(w, 7, h, border=1, align="C", fill=True)
    pdf.ln()
    pdf.set_font("Helvetica", "", 8)

    def format_cell(value):
        if pd.isna(value) or value is None: return '-'
        try:
            return f"{value:.1f}"
        except:
            return str(value)

    for _, row in df_ultimos.iterrows():
        pdf.cell(col_widths[0], 6, row['timestamp_local_str'], border=1, align="C")
        pdf.cell(col_widths[1], 6, format_cell(row['chuva_mm']), border=1, align="C")
        pdf.cell(col_widths[2], 6, format_cell(row['umidade_1m_perc']), border=1, align="C")
        pdf.cell(col_widths[3], 6, format_cell(row['umidade_2m_perc']), border=1, align="C")
        pdf.cell(col_widths[4], 6, format_cell(row['umidade_3m_perc']), border=1, align="C")
        pdf.ln()
    return pdf.output(dest='S')


def criar_relatorio_logs_em_memoria(nome_ponto, logs_filtrados):
    # ... (código idêntico, sem alterações) ...
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=10)
    pdf.add_page()
    pdf.set_font("Helvetica", size=10)
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, f"Histórico de Eventos - {nome_ponto}", ln=True, align="C")
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 5, f"Período: Últimos 7 dias (ou total dos logs)", ln=True, align="C")
    pdf.cell(0, 5, f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}", ln=True, align="C")
    pdf.ln(5)
    pdf.set_font("Courier", size=8)
    largura_disponivel = pdf.w - pdf.l_margin - pdf.r_margin
    for log_str in reversed(logs_filtrados):
        try:
            parts = log_str.split('|')
            timestamp_str_utc_iso, ponto_str, msg_str = parts[0].strip(), parts[1].strip(), "|".join(parts[2:]).strip()
            try:
                dt_utc = pd.to_datetime(timestamp_str_utc_iso).tz_localize('UTC')
                dt_local = dt_utc.tz_convert('America/Sao_Paulo')
                timestamp_formatado = dt_local.strftime('%d/%m/%Y %H:%M:%S')
            except Exception:
                timestamp_formatado = timestamp_str_utc_iso.split('+')[0].replace('T', ' ')
            cor = (0, 0, 0)
            if "ERRO" in msg_str:
                cor = (200, 0, 0)
            elif "AVISO" in msg_str:
                cor = (200, 150, 0)
            elif "MUDANÇA" in msg_str:
                cor = (0, 0, 200)
            pdf.set_text_color(*cor)
            linha = f"[{timestamp_formatado}] {ponto_str}: {msg_str}"
            pdf.multi_cell(largura_disponivel, 4, linha, ln=1)
        except Exception:
            pdf.set_text_color(0, 0, 0)
            pdf.multi_cell(largura_disponivel, 4, log_str, ln=1)
    pdf.set_text_color(0, 0, 0)
    return pdf.output(dest='S')


def gerar_relatorio_dados_direto(start_date, end_date, id_ponto):
    try:
        data_inicio_str, data_fim_str = pd.to_datetime(start_date).strftime('%d/%m/%Y'), pd.to_datetime(
            end_date).strftime('%d/%m/%Y')
        periodo_str = f"({data_inicio_str} a {data_fim_str})"
        start_dt_local = pd.to_datetime(start_date).tz_localize('America/Sao_Paulo')
        start_dt = start_dt_local.tz_convert('UTC')
        end_dt_local = pd.to_datetime(end_date).tz_localize('America/Sao_Paulo')
        end_dt_local_final = end_dt_local + pd.Timedelta(days=1)
        end_dt = end_dt_local_final.tz_convert('UTC')

        df_filtrado = data_source.read_data_from_db(id_ponto, start_dt, end_dt)

        if df_filtrado.empty:
            return None, None, "Sem dados no período selecionado."
        if df_filtrado['timestamp'].dt.tz is None:
            df_filtrado['timestamp'] = pd.to_datetime(df_filtrado['timestamp']).dt.tz_localize('UTC')

        # --- INÍCIO DA CORREÇÃO ---
        # Removido o '_' extra de 'dt_tz_convert'
        df_filtrado.loc[:, 'timestamp_local'] = df_filtrado['timestamp'].dt.tz_convert('America/Sao_Paulo')
        # --- FIM DA CORREÇÃO ---

        config = PONTOS_DE_ANALISE.get(id_ponto, {"nome": "Ponto"})

        status_texto, status_cor = "Relatório de Dados", "secondary"
        df_filtrado['chuva_mm'] = pd.to_numeric(df_filtrado['chuva_mm'], errors='coerce').fillna(0)
        df_filtrado['chuva_acum_periodo'] = df_filtrado['chuva_mm'].cumsum()
        df_chuva_72h_pdf = processamento.calcular_acumulado_rolling(df_filtrado, horas=72)
        if 'timestamp' in df_chuva_72h_pdf.columns:
            if df_chuva_72h_pdf['timestamp'].dt.tz is None: df_chuva_72h_pdf.loc[:, 'timestamp'] = df_chuva_72h_pdf[
                'timestamp'].dt.tz_localize('UTC')

            # --- INÍCIO DA CORREÇÃO ---
            # Removido o '_' extra de 'dt_tz_convert'
            df_chuva_72h_pdf.loc[:, 'timestamp_local'] = df_chuva_72h_pdf['timestamp'].dt.tz_convert(
                'America/Sao_Paulo')
            # --- FIM DA CORREÇÃO ---
        else:
            df_chuva_72h_pdf = df_chuva_72h_pdf.copy()
            df_chuva_72h_pdf.loc[:, 'timestamp_local'] = df_chuva_72h_pdf['timestamp']

        # (O resto da função é idêntico)
        largura_barra_dias = 1 / 144
        fig_chuva_mp, ax1 = plt.subplots(figsize=(10, 5))
        ax1.bar(df_filtrado['timestamp_local'], df_filtrado['chuva_mm'], color='#5F6B7C', alpha=0.8,
                label='Pluv. Horária (mm)', width=largura_barra_dias, align='center')
        ax1.set_xlabel("Data e Hora");
        ax1.set_ylabel("Pluviometria Horária (mm)", color='#2C3E50')
        ax1.tick_params(axis='y', labelcolor='#2C3E50');
        ax1.tick_params(axis='x', rotation=45, labelsize=8)
        ax1.grid(True, linestyle='--', alpha=0.6, which='both')
        ax2 = ax1.twinx()
        ax2.plot(df_chuva_72h_pdf['timestamp_local'], df_chuva_72h_pdf['chuva_mm'], color='#007BFF', linewidth=2.5,
                 label='Acumulada (72h)')
        ax2.plot(df_filtrado['timestamp_local'], df_filtrado['chuva_acum_periodo'], color='red', linewidth=2.0,
                 linestyle='--', label='Acumulada (Período)')
        ax2.set_ylabel("Acumulada (72h)", color='#007BFF');
        ax2.tick_params(axis='y', labelcolor='#007BFF')
        fig_chuva_mp.suptitle(f"Pluviometria - Estação {config['nome']}", fontsize=12)
        lines, labels = ax1.get_legend_handles_labels();
        lines2, labels2 = ax2.get_legend_handles_labels()
        fig_chuva_mp.legend(lines + lines2, labels + labels2, loc='upper center', ncol=3, fancybox=True, shadow=True,
                            bbox_to_anchor=(0.5, 0.1))
        fig_chuva_mp.subplots_adjust(bottom=0.25, top=0.9)
        fig_umidade_mp, ax_umidade = plt.subplots(figsize=(10, 5))
        from pages.specific_dash import CORES_ALERTAS_CSS
        ax_umidade.plot(df_filtrado['timestamp_local'], df_filtrado['umidade_1m_perc'], label='1m',
                        color=CORES_ALERTAS_CSS['verde'], linewidth=2)
        ax_umidade.plot(df_filtrado['timestamp_local'], df_filtrado['umidade_2m_perc'], label='2m',
                        color=CORES_ALERTAS_CSS['laranja'], linewidth=2)
        ax_umidade.plot(df_filtrado['timestamp_local'], df_filtrado['umidade_3m_perc'], label='3m',
                        color=CORES_ALERTAS_CSS['vermelho'], linewidth=2)
        ax_umidade.set_title(f"Variação da Umidade do Solo - Estação {config['nome']}", fontsize=12)
        ax_umidade.set_xlabel("Data e Hora");
        ax_umidade.set_ylabel("Umidade do Solo (%)")
        lines, labels = ax_umidade.get_legend_handles_labels()
        fig_umidade_mp.legend(lines, labels, loc='upper center', bbox_to_anchor=(0.5, 0.1), ncol=3, fancybox=True,
                              shadow=True)
        plt.grid(True, linestyle='--', alpha=0.6);
        ax_umidade.tick_params(axis='x', rotation=45, labelsize=8)
        fig_umidade_mp.subplots_adjust(bottom=0.25, top=0.9)
        pdf_buffer = criar_relatorio_em_memoria(df_filtrado, fig_chuva_mp, fig_umidade_mp, status_texto, status_cor,
                                                periodo_str)
        plt.close(fig_chuva_mp);
        plt.close(fig_umidade_mp)
        nome_arquivo = f"Relatorio_{config['nome']}_{datetime.now().strftime('%Y%m%d')}.pdf"
        return pdf_buffer, nome_arquivo, None
    except Exception as e:
        print(f"ERRO CRÍTICO ao gerar PDF de dados:")
        traceback.print_exc()
        return None, None, str(e)


def thread_gerar_excel(task_id, start_date, end_date, id_ponto):
    # (Esta função já estava correta e funcionando, sem alterações)
    try:
        start_dt_local = pd.to_datetime(start_date).tz_localize('America/Sao_Paulo')
        start_dt = start_dt_local.tz_convert('UTC')
        end_dt_local = pd.to_datetime(end_date).tz_localize('America/Sao_Paulo')
        end_dt_local_final = end_dt_local + pd.Timedelta(days=1)
        end_dt = end_dt_local_final.tz_convert('UTC')

        df_filtrado = data_source.read_data_from_db(id_ponto, start_dt, end_dt)

        if df_filtrado.empty: raise Exception("Sem dados no período selecionado.")
        if df_filtrado['timestamp'].dt.tz is None:
            df_filtrado.loc[:, 'timestamp'] = pd.to_datetime(df_filtrado['timestamp']).dt_tz_localize('UTC')
        df_filtrado.loc[:, 'Data/Hora (Local)'] = df_filtrado['timestamp'].dt.tz_convert(
            'America/Sao_Paulo').dt.strftime('%d/%m/%Y %H:%M:%S')
        df_filtrado = df_filtrado.drop(columns=['timestamp'])
        colunas_renomeadas = {'id_ponto': 'ID Ponto', 'chuva_mm': 'Chuva (mm/h)',
                              'precipitacao_acumulada_mm': 'Precipitação Acumulada (mm)',
                              'umidade_1m_perc': 'Umidade 1m (%)', 'umidade_2m_perc': 'Umidade 2m (%)',
                              'umidade_3m_perc': 'Umidade 3m (%)', 'base_1m': 'Base Umidade 1m',
                              'base_2m': 'Base Umidade 2m', 'base_3m': 'Base Umidade 3m'}
        df_filtrado = df_filtrado.rename(columns=colunas_renomeadas)
        colunas_ordenadas = ['ID Ponto', 'Data/Hora (Local)'] + [col for col in df_filtrado.columns if
                                                                 col not in ['ID Ponto', 'Data/Hora (Local)']]
        df_filtrado = df_filtrado[colunas_ordenadas]
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        df_filtrado.to_excel(writer, sheet_name='Dados Históricos', index=False)
        writer.close()
        output.seek(0)
        excel_data = output.read()
        config = PONTOS_DE_ANALISE.get(id_ponto, {"nome": "Ponto"})
        nome_arquivo = f"Dados_Historicos_{config['nome']}_{datetime.now().strftime('%Y%m%d')}.xlsx"
        print(f"[Thread Excel {task_id}] Excel gerado com sucesso.")
        with EXCEL_CACHE_LOCK:
            EXCEL_CACHE[task_id] = {"status": "concluido", "data": excel_data, "filename": nome_arquivo}
    except Exception as e:
        print(f"ERRO CRÍTICO [Thread Excel {task_id}]:")
        traceback.print_exc()
        with EXCEL_CACHE_LOCK:
            EXCEL_CACHE[task_id] = {"status": "erro", "message": str(e)}


def thread_gerar_pdf(task_id, start_date, end_date, id_ponto):
    # (Esta função já estava correta, sem alterações)
    try:
        pdf_buffer, nome_arquivo, error_msg = gerar_relatorio_dados_direto(
            start_date, end_date, id_ponto
        )
        if error_msg:
            raise Exception(error_msg)

        print(f"[Thread PDF {task_id}] PDF gerado com sucesso.")
        with PDF_CACHE_LOCK:
            PDF_CACHE[task_id] = {"status": "concluido", "data": pdf_buffer, "filename": nome_arquivo}
    except Exception as e:
        print(f"ERRO CRÍTICO [Thread PDF {task_id}]:")
        traceback.print_exc()
        with PDF_CACHE_LOCK:
            PDF_CACHE[task_id] = {"status": "erro", "message": str(e)}