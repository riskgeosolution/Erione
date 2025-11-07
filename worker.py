# worker.py (ATUALIZADO E COMPLETO)
# Este é o único processo de worker, com a lógica de cálculo correta
# e os novos níveis de status ("OBSERVAÇÃO", "ALERTA MÁXIMO", etc.)

import pandas as pd
import time
import datetime
import data_source
import processamento
import alertas
import json
import traceback
import os
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

from config import (
    PONTOS_DE_ANALISE, RISCO_MAP, FREQUENCIA_API_SEGUNDOS, CONSTANTES_PADRAO,
    STATUS_MAP_HIERARQUICO
)


# ==============================================================================
# --- LÓGICA DE ALERTA ---
# ==============================================================================

def worker_verificar_alertas(status_novos_dict, status_antigos_dict):
    """
    Compara o status antigo com o novo e dispara alertas se necessário.
    Usa o status "geral".
    """
    default_status = {"geral": "INDEFINIDO", "chuva": "INDEFINIDO", "umidade": "INDEFINIDO",
                      "inclinometro_x": "INDEFINIDO", "inclinometro_y": "INDEFINIDO"}

    if not status_novos_dict:
        print("[Worker] Nenhum status novo recebido para verificação.")
        return status_antigos_dict

    if not isinstance(status_antigos_dict, dict) or not all(
            isinstance(v, dict) for v in status_antigos_dict.values()):
        status_antigos_dict = {pid: default_status for pid in PONTOS_DE_ANALISE.keys()}

    status_atualizado = status_antigos_dict.copy()

    for id_ponto in PONTOS_DE_ANALISE.keys():
        status_novo_info = status_novos_dict.get(id_ponto, default_status)
        status_antigo_info = status_antigos_dict.get(id_ponto, default_status)

        # Compara o status GERAL
        status_novo = status_novo_info.get("geral", "SEM DADOS")
        status_antigo = status_antigo_info.get("geral", "INDEFINIDO")

        if status_novo != status_antigo:
            try:
                nome_ponto = PONTOS_DE_ANALISE[id_ponto]['nome']
                mensagem_log = f"MUDANÇA DE STATUS: {nome_ponto} mudou de {status_antigo} para {status_novo}."
                data_source.adicionar_log(id_ponto, mensagem_log)
                print(f"| {id_ponto} | {mensagem_log}")

                # --- Dispara o alerta ---
                alertas.enviar_alerta(id_ponto, nome_ponto, status_novo, status_antigo)
                # -------------------------

            except Exception as e:
                print(f"Erro ao gerar log/alerta de mudança de status: {e}")

            status_atualizado[id_ponto] = status_novo_info
        else:
            status_atualizado[id_ponto] = status_antigo_info

    return status_atualizado


# ==============================================================================
# --- LÓGICA PRINCIPAL DO WORKER ---
# ==============================================================================

def worker_main_loop():
    """
    Executa UM ciclo de coleta e processamento.
    Usa a lógica completa (Chuva, Umidade, Inclinômetro)
    e os novos níveis de status.
    """
    inicio_ciclo = time.time()
    try:
        # 1. Worker lê o CSV (cache 72h) APENAS para o gap-check
        historico_df_cache_csv, status_antigos_do_disco, logs = data_source.get_all_data_from_disk(worker_mode=True)

        default_status = {"geral": "INDEFINIDO", "chuva": "INDEFINIDO", "umidade": "INDEFINIDO",
                          "inclinometro_x": "INDEFINIDO", "inclinometro_y": "INDEFINIDO"}
        if not status_antigos_do_disco or not isinstance(list(status_antigos_do_disco.values())[0], dict):
            status_antigos_do_disco = {p: default_status for p in PONTOS_DE_ANALISE.keys()}

        print(f"WORKER: Início do ciclo. Histórico (cache CSV) lido: {len(historico_df_cache_csv)} entradas.")

        # --- LÓGICA DE BACKFILL (GAP CHECK) ---
        # (Esta lógica foi pega do seu 'index.py' antigo)
        df_ponto_cache = historico_df_cache_csv[historico_df_cache_csv['id_ponto'] == 'Ponto-1']
        run_backfill = False
        if df_ponto_cache.empty:
            print("[Worker] Histórico do Ponto-1 está vazio. Tentando backfill...")
            run_backfill = True
        else:
            try:
                if not pd.api.types.is_datetime64_any_dtype(df_ponto_cache['timestamp']):
                    df_ponto_cache.loc[:, 'timestamp'] = pd.to_datetime(df_ponto_cache['timestamp'], utc=True)

                latest_timestamp = df_ponto_cache['timestamp'].max()
                if latest_timestamp.tzinfo is None:
                    latest_timestamp = latest_timestamp.tz_localize('UTC')

                agora_utc = datetime.datetime.now(datetime.timezone.utc)

                gap_segundos = (agora_utc - latest_timestamp).total_seconds()
                if gap_segundos > (FREQUENCIA_API_SEGUNDOS + 60):
                    print(
                        f"[Worker] Detectado 'gap' de {gap_segundos / 60:.0f} min para o Ponto-1. Rodando backfill...")
                    run_backfill = True
            except Exception as e_gap:
                print(f"[Worker] Erro ao checar 'gap' de dados: {e_gap}. Pulando backfill.")

        # (O backfill real não estava implementado, mas o gap check sim)

        # 2. COLETAR NOVOS DADOS DA API
        # A função usa o cache CSV (historico_df_cache_csv) para saber de onde parar
        novos_dados_df, _ = data_source.executar_passo_api_e_salvar(historico_df_cache_csv)

        # 3. RECARREGAR O HISTÓRICO COMPLETO DO SQLITE (14 DIAS) PARA CÁLCULO DE STATUS
        historico_completo_db, _, _ = data_source.get_all_data_from_disk(worker_mode=False)

        if historico_completo_db.empty:
            print("AVISO (Worker): Histórico (SQLite) vazio, pulando cálculo de status.")
            status_atualizado = {p: default_status for p in PONTOS_DE_ANALISE.keys()}
        else:
            status_atualizado = {}
            for id_ponto in PONTOS_DE_ANALISE.keys():
                df_ponto = historico_completo_db[historico_completo_db['id_ponto'] == id_ponto].copy()

                if df_ponto.empty:
                    status_atualizado[id_ponto] = default_status
                    continue

                # 4a. Status da Chuva
                acumulado_72h_df = processamento.calcular_acumulado_rolling(df_ponto, horas=72)
                if not acumulado_72h_df.empty:
                    chuva_72h_final = acumulado_72h_df['chuva_mm'].iloc[-1]
                    status_ponto_chuva, _ = processamento.definir_status_chuva(chuva_72h_final)
                    risco_chuva = RISCO_MAP.get(status_ponto_chuva, -1)
                else:
                    status_ponto_chuva = "SEM DADOS"
                    risco_chuva = -1

                # 4b. Status da Umidade e Inclinômetro
                try:
                    for col in ['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc', 'inclinometro_x',
                                'inclinometro_y']:
                        if col not in df_ponto.columns: df_ponto[col] = pd.NA

                    ultimo_dado_incli_valido = df_ponto.dropna(subset=['inclinometro_x', 'inclinometro_y']).sort_values(
                        'timestamp')
                    ultimo_dado_umid_valido = df_ponto.dropna(
                        subset=['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc']).sort_values('timestamp')

                    constantes_ponto = PONTOS_DE_ANALISE[id_ponto].get('constantes', CONSTANTES_PADRAO)
                    base_1m = constantes_ponto.get('UMIDADE_BASE_1M', CONSTANTES_PADRAO['UMIDADE_BASE_1M'])
                    base_2m = constantes_ponto.get('UMIDADE_BASE_2M', CONSTANTES_PADRAO['UMIDADE_BASE_2M'])
                    base_3m = constantes_ponto.get('UMIDADE_BASE_3M', CONSTANTES_PADRAO['UMIDADE_BASE_3M'])

                    if not ultimo_dado_umid_valido.empty:
                        umidade_1m = ultimo_dado_umid_valido.iloc[-1].get('umidade_1m_perc', base_1m)
                        umidade_2m = ultimo_dado_umid_valido.iloc[-1].get('umidade_2m_perc', base_2m)
                        umidade_3m = ultimo_dado_umid_valido.iloc[-1].get('umidade_3m_perc', base_3m)
                    else:
                        umidade_1m, umidade_2m, umidade_3m = pd.NA, pd.NA, pd.NA

                    # (Note: A função definir_status_umidade_hierarquico usa os novos nomes/limites do config.py)
                    status_ponto_umidade, _, _ = processamento.definir_status_umidade_hierarquico(
                        umidade_1m, umidade_2m, umidade_3m, base_1m, base_2m, base_3m
                    )
                    risco_umidade = RISCO_MAP.get(status_ponto_umidade, -1)

                    if not ultimo_dado_incli_valido.empty:
                        inclinometro_x = ultimo_dado_incli_valido.iloc[-1].get('inclinometro_x')
                        inclinometro_y = ultimo_dado_incli_valido.iloc[-1].get('inclinometro_y')
                    else:
                        inclinometro_x, inclinometro_y = pd.NA, pd.NA

                    base_x = constantes_ponto.get('INCLINOMETRO_BASE_X', -17.7)
                    base_y = constantes_ponto.get('INCLINOMETRO_BASE_Y', 8.3)

                    # (Note: A função definir_status_inclinometro_individual usa os novos nomes/limites do config.py)
                    status_ponto_inclinometro_x, _ = processamento.definir_status_inclinometro_individual(
                        inclinometro_x, base_x)
                    status_ponto_inclinometro_y, _ = processamento.definir_status_inclinometro_individual(
                        inclinometro_y, base_y)

                    risco_inclinometro_x = RISCO_MAP.get(status_ponto_inclinometro_x, -1)
                    risco_inclinometro_y = RISCO_MAP.get(status_ponto_inclinometro_y, -1)

                except Exception as e_umid:
                    print(f"[Worker] Erro ao calcular status de umidade/inclinômetro: {e_umid}")
                    status_ponto_umidade = "INDEFINIDO"
                    risco_umidade = -1
                    status_ponto_inclinometro_x = "INDEFINIDO"
                    risco_inclinometro_x = -1
                    status_ponto_inclinometro_y = "INDEFINIDO"
                    risco_inclinometro_y = -1

                # 4c. Status Geral
                risco_geral = max(risco_chuva, risco_umidade, risco_inclinometro_x, risco_inclinometro_y)
                # Pega o NOME do status (Ex: "ALERTA MÁXIMO")
                status_geral_texto = STATUS_MAP_HIERARQUICO.get(risco_geral, STATUS_MAP_HIERARQUICO[-1])[0]

                # 5. Salva o dicionário completo de status
                status_atualizado[id_ponto] = {
                    "geral": status_geral_texto,
                    "chuva": status_ponto_chuva,
                    "umidade": status_ponto_umidade,
                    "inclinometro_x": status_ponto_inclinometro_x,
                    "inclinometro_y": status_ponto_inclinometro_y
                }

        # 6. Compara o status novo com o antigo e dispara alertas
        status_final_com_alertas = worker_verificar_alertas(status_atualizado, status_antigos_do_disco)

        # 7. Salva o status final no disco (para o site ler)
        try:
            with open(data_source.STATUS_FILE, 'w', encoding='utf-8') as f:
                json.dump(status_final_com_alertas, f, indent=2)
        except Exception as e:
            print(f"ERRO CRÍTICO (Worker) ao salvar status: {e}")
            traceback.print_exc()
            data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (Worker) ao salvar status: {e}")

        print(f"WORKER: Ciclo concluído em {time.time() - inicio_ciclo:.2f}s.")
        return True

    except Exception as e:
        print(f"WORKER ERRO CRÍTICO no loop principal: {e}")
        traceback.print_exc()
        data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (Worker loop): {e}")
        return False


# ==============================================================================
# --- LOOP DE EXECUÇÃO (Scheduler) ---
# ==============================================================================

def background_task_wrapper():
    """
    Função "scheduler" que chama o loop principal
    e dorme de forma inteligente até o próximo ciclo.
    """
    data_source.setup_disk_paths()
    print("--- Processo Worker (worker.py) Iniciado (Modo Sincronizado) ---")
    data_source.adicionar_log("GERAL", "Processo Worker (worker.py) iniciado com sucesso.")

    INTERVALO_EM_MINUTOS = 15
    CARENCIA_EM_SEGUNDOS = 60  # 1 minuto de "folga"

    while True:
        inicio_total = time.time()

        # 1. Roda o ciclo de coleta
        worker_main_loop()

        tempo_execucao = time.time() - inicio_total

        # 2. Pega a hora atual (UTC)
        agora_utc = datetime.datetime.now(datetime.timezone.utc)

        # 3. Calcula quando o *próximo* ciclo de 15 min começa
        proximo_minuto_base = (agora_utc.minute // INTERVALO_EM_MINUTOS + 1) * INTERVALO_EM_MINUTOS
        proxima_hora_utc = agora_utc

        # 4. Lida com a virada da hora (ex: 10:47 -> 11:00)
        if proximo_minuto_base >= 60:
            proxima_hora_utc = agora_utc + datetime.timedelta(hours=1)
            proximo_minuto_base = 0

        # 5. Define a hora exata da próxima execução (a hora "base")
        proxima_execucao_base_utc = proxima_hora_utc.replace(
            minute=proximo_minuto_base,
            second=0,
            microsecond=0
        )

        # 6. Adiciona o período de carência (a "folga")
        proxima_execucao_com_carencia_utc = proxima_execucao_base_utc + datetime.timedelta(seconds=CARENCIA_EM_SEGUNDOS)

        # 7. Calcula quantos segundos dormir até lá
        tempo_para_dormir_seg = (proxima_execucao_com_carencia_utc - agora_utc).total_seconds()

        # 8. Garante que não dormimos um tempo negativo
        if tempo_para_dormir_seg < 0:
            print(f"AVISO (Worker): O ciclo demorou {tempo_execucao:.1f}s e perdeu a janela. Rodando novamente...")
            tempo_para_dormir_seg = 1

        print(f"WORKER: Ciclo levou {tempo_execucao:.1f}s.")
        print(
            f"WORKER: Próxima execução às {proxima_execucao_com_carencia_utc.isoformat()}. Dormindo por {tempo_para_dormir_seg:.0f}s...")
        time.sleep(tempo_para_dormir_seg)


# Ponto de entrada principal do script worker.py
if __name__ == "__main__":
    background_task_wrapper()