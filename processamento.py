# processamento.py (ATUALIZADO: Lógica de status do inclinômetro dividida)

import pandas as pd
import datetime
import traceback

from config import (
    CHUVA_LIMITE_VERDE, CHUVA_LIMITE_AMARELO, CHUVA_LIMITE_LARANJA,
    DELTA_TRIGGER_UMIDADE, RISCO_MAP, STATUS_MAP_HIERARQUICO,
    FREQUENCIA_API_SEGUNDOS,
    # --- INÍCIO DA ATUALIZAÇÃO ---
    INCLINOMETRO_DELTA_AMARELO, INCLINOMETRO_DELTA_LARANJA  # Nomes atualizados
    # --- FIM DA ATUALIZAÇÃO ---
)


# (A função calcular_acumulado_rolling permanece a mesma)
def calcular_acumulado_rolling(df_ponto, horas=72):
    if 'chuva_mm' not in df_ponto.columns or df_ponto.empty or 'timestamp' not in df_ponto.columns:
        return pd.DataFrame(columns=['id_ponto', 'timestamp', 'chuva_mm'])
    df_original = df_ponto.sort_values('timestamp').copy()
    try:
        df_original['timestamp'] = pd.to_datetime(df_original['timestamp'])
        df_original = df_original.set_index('timestamp')
        df_original['chuva_mm'] = pd.to_numeric(df_original['chuva_mm'], errors='coerce')

        lista_dfs_acumulados = []
        PONTOS_POR_HORA = 3600 // FREQUENCIA_API_SEGUNDOS
        window_size = int(horas * PONTOS_POR_HORA)

        def calcular_rolling_para_serie(serie_chuva):
            df_resampled = serie_chuva.resample('15T').sum()
            df_resampled = df_resampled.fillna(0)
            acumulado = df_resampled.rolling(window=window_size, min_periods=1).sum()
            return acumulado

        for ponto_id in df_original['id_ponto'].unique():
            series_ponto = df_original[df_original['id_ponto'] == ponto_id]['chuva_mm']
            acumulado_series = calcular_rolling_para_serie(series_ponto)
            acumulado_df = acumulado_series.to_frame(name='chuva_mm')
            acumulado_df['id_ponto'] = ponto_id
            lista_dfs_acumulados.append(acumulado_df)

        df_final = pd.concat(lista_dfs_acumulados)
        return df_final.reset_index()
    except Exception as e:
        print(f"Erro CRÍTICO ao calcular acumulado rolling: {e}")
        traceback.print_exc()
        return pd.DataFrame(columns=['id_ponto', 'timestamp', 'chuva_mm'])


# (A função definir_status_chuva permanece a mesma)
def definir_status_chuva(chuva_mm):
    STATUS_MAP_CHUVA = {"LIVRE": "success", "ATENÇÃO": "warning", "ALERTA": "orange", "PARALIZAÇÃO": "danger",
                        "SEM DADOS": "secondary", "INDEFINIDO": "secondary"}
    try:
        if pd.isna(chuva_mm):
            status_texto = "SEM DADOS"
        elif chuva_mm >= CHUVA_LIMITE_LARANJA:
            status_texto = "PARALIZAÇÃO"
        elif chuva_mm > CHUVA_LIMITE_AMARELO:
            status_texto = "ALERTA"
        elif chuva_mm > CHUVA_LIMITE_VERDE:
            status_texto = "ATENÇÃO"
        else:
            status_texto = "LIVRE"
        return status_texto, STATUS_MAP_CHUVA.get(status_texto, "secondary")
    except Exception as e:
        print(f"Erro status chuva: {e}");
        return "INDEFINIDO", "secondary"


# (A função definir_status_umidade_hierarquico permanece a mesma)
def definir_status_umidade_hierarquico(umidade_1m, umidade_2m, umidade_3m,
                                       base_1m, base_2m, base_3m,
                                       chuva_acumulada_72h=0.0):
    try:
        if pd.isna(umidade_1m) or pd.isna(umidade_2m) or pd.isna(umidade_3m) or \
                pd.isna(base_1m) or pd.isna(base_2m) or pd.isna(base_3m):
            return STATUS_MAP_HIERARQUICO[-1]

        s1_sim = (umidade_1m - base_1m) >= DELTA_TRIGGER_UMIDADE
        s2_sim = (umidade_2m - base_2m) >= DELTA_TRIGGER_UMIDADE
        s3_sim = (umidade_3m - base_3m) >= DELTA_TRIGGER_UMIDADE

        risco_final = 0
        if s1_sim and s2_sim and s3_sim:
            risco_final = 3
        elif (s1_sim and s2_sim and not s3_sim) or \
                (not s1_sim and s2_sim and s3_sim):
            risco_final = 2
        elif (s1_sim and not s2_sim and not s3_sim) or \
                (not s1_sim and not s2_sim and s3_sim):
            risco_final = 1

        return STATUS_MAP_HIERARQUICO[risco_final]
    except Exception as e:
        print(f"Erro ao definir status de umidade solo (fluxograma): {e}")
        return STATUS_MAP_HIERARQUICO[-1]


# --- INÍCIO DA ATUALIZAÇÃO (Função agora é INDIVIDUAL) ---
def definir_status_inclinometro_individual(inclinometro_val, base_val):
    """
    Define o status de alerta para um ÚNICO eixo (X ou Y)
    com base na VARIAÇÃO (delta) em relação à sua base.
    """
    STATUS_MAP_INCLINOMETRO = {"LIVRE": "success", "ATENÇÃO": "warning", "ALERTA": "orange", "PARALIZAÇÃO": "danger",
                               "SEM DADOS": "secondary", "INDEFINIDO": "secondary"}
    try:
        if pd.isna(inclinometro_val) or pd.isna(base_val):
            status_texto = "SEM DADOS"
        else:
            # Calcula a variação absoluta (delta)
            delta = abs(inclinometro_val - base_val)

            if delta >= INCLINOMETRO_DELTA_LARANJA:  # >= 10.0 graus de variação
                status_texto = "PARALIZAÇÃO"
            elif delta > INCLINOMETRO_DELTA_AMARELO:  # > 5.0 graus de variação
                status_texto = "ALERTA"
            else:
                status_texto = "LIVRE"

        return status_texto, STATUS_MAP_INCLINOMETRO.get(status_texto, "secondary")

    except Exception as e:
        print(f"Erro status inclinômetro: {e}");
        return "INDEFINIDO", "secondary"


# --- FIM DA ATUALIZAÇÃO ---


# (Funções definir_status_umidade_individual e ler_logs_eventos permanecem as mesmas)
def definir_status_umidade_individual(umidade_atual, umidade_base, risco_nivel):
    try:
        if pd.isna(umidade_atual) or pd.isna(umidade_base):
            return "grey"
        if (umidade_atual - umidade_base) >= DELTA_TRIGGER_UMIDADE:
            if risco_nivel == 1:
                return "#FFD700"
            elif risco_nivel == 2:
                return "#fd7e14"
            elif risco_nivel == 3:
                return "#dc3545"
            else:
                return "#FFD700"
        else:
            return "green"
    except Exception:
        return "grey"


def ler_logs_eventos(id_ponto):
    import data_source
    try:
        _, _, logs_str = data_source.get_all_data_from_disk()
        logs_list = logs_str.split('\n')
        logs_filtrados = [log for log in logs_list if f"| {id_ponto} |" in log or "| GERAL |" in log]
        return '\n'.join(logs_filtrados)
    except Exception as e:
        return f"ERRO ao ler logs: {e}"