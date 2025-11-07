# index.py (CORRIGIDO: Worker agora calcula status a partir do SQLite)

import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
from io import StringIO
import os
import json
from dotenv import load_dotenv
import time
from threading import Thread
import datetime

load_dotenv()

from app import app, server

from pages import login as login_page
from pages import main_app as main_app_page
from pages import map_view, general_dash, specific_dash
import data_source
import config

import processamento
import alertas
import traceback
from config import PONTOS_DE_ANALISE, RISCO_MAP, FREQUENCIA_API_SEGUNDOS, CONSTANTES_PADRAO, STATUS_MAP_HIERARQUICO

# --- SENHA DO APP (LOGIN) ---
SENHA_CLIENTE = '123'
SENHA_ADMIN = 'admin456'


# ==============================================================================
# --- LÓGICA DO WORKER (COM ATUALIZAÇÃO DE STATUS) ---
# ==============================================================================

def worker_verificar_alertas(status_novos_dict, status_antigos_dict):
    default_status = {"geral": "INDEFINIDO", "chuva": "INDEFINIDO", "umidade": "INDEFINIDO",
                      "inclinometro_x": "INDEFINIDO", "inclinometro_y": "INDEFINIDO"}
    if not status_novos_dict:
        print("[Worker Thread] Nenhum status novo recebido para verificação.")
        return status_antigos_dict
    if not isinstance(status_antigos_dict, dict) or not all(isinstance(v, dict) for v in status_antigos_dict.values()):
        status_antigos_dict = {pid: default_status for pid in PONTOS_DE_ANALISE.keys()}

    status_atualizado = status_antigos_dict.copy()

    for id_ponto in PONTOS_DE_ANALISE.keys():
        status_novo_info = status_novos_dict.get(id_ponto, default_status)
        status_antigo_info = status_antigos_dict.get(id_ponto, default_status)
        status_novo = status_novo_info.get("geral", "SEM DADOS")
        status_antigo = status_antigo_info.get("geral", "INDEFINIDO")

        if status_novo != status_antigo:
            try:
                nome_ponto = PONTOS_DE_ANALISE[id_ponto]['nome']
                mensagem_log = f"MUDANÇA DE STATUS: {nome_ponto} mudou de {status_antigo} para {status_novo}."
                data_source.adicionar_log(id_ponto, mensagem_log)
                print(f"| {id_ponto} | {mensagem_log}")
            except Exception as e:
                print(f"Erro ao gerar log de mudança de status: {e}")
            status_atualizado[id_ponto] = status_novo_info
        else:
            status_atualizado[id_ponto] = status_antigo_info
    return status_atualizado


def worker_main_loop():
    inicio_ciclo = time.time()
    try:
        # 1. Worker lê o CSV (cache 72h) APENAS para o gap-check
        historico_df_cache_csv, status_antigos_do_disco, logs = data_source.get_all_data_from_disk(worker_mode=True)

        default_status = {"geral": "INDEFINIDO", "chuva": "INDEFINIDO", "umidade": "INDEFINIDO",
                          "inclinometro_x": "INDEFINIDO", "inclinometro_y": "INDEFINIDO"}
        if not status_antigos_do_disco or not isinstance(list(status_antigos_do_disco.values())[0], dict):
            status_antigos_do_disco = {p: default_status for p in PONTOS_DE_ANALISE.keys()}

        print(f"WORKER (Thread): Início do ciclo. Histórico (cache CSV) lido: {len(historico_df_cache_csv)} entradas.")

        # --- LÓGICA DE BACKFILL (GAP CHECK) ---
        df_ponto_cache = historico_df_cache_csv[historico_df_cache_csv['id_ponto'] == 'Ponto-1']
        run_backfill = False
        if df_ponto_cache.empty:
            print("[Worker Thread] Histórico do Ponto-1 está vazio. Tentando backfill...")
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
                        f"[Worker Thread] Detectado 'gap' de {gap_segundos / 60:.0f} min para o Ponto-1. Rodando backfill...")
                    run_backfill = True
            except Exception as e_gap:
                print(f"[Worker Thread] Erro ao checar 'gap' de dados: {e_gap}. Pulando backfill.")

        # 2. COLETAR NOVOS DADOS DA API
        # A função usa o cache CSV (historico_df_cache_csv) para saber de onde parar
        novos_dados_df, _ = data_source.executar_passo_api_e_salvar(historico_df_cache_csv)

        # --- INÍCIO DA ATUALIZAÇÃO (Worker agora lê do DB) ---
        # 3. RECARREGAR O HISTÓRICO COMPLETO DO SQLITE (14 DIAS) PARA CÁLCULO DE STATUS
        historico_completo_db, _, _ = data_source.get_all_data_from_disk(worker_mode=False)
        # --- FIM DA ATUALIZAÇÃO ---

        if historico_completo_db.empty:
            print("AVISO (Thread): Histórico (SQLite) vazio, pulando cálculo de status.")
            status_atualizado = {p: default_status for p in PONTOS_DE_ANALISE.keys()}
        else:
            status_atualizado = {}
            for id_ponto in PONTOS_DE_ANALISE.keys():
                # --- INÍCIO DA ATUALIZAÇÃO ---
                # Calcula o status usando o DF do SQLite
                df_ponto = historico_completo_db[historico_completo_db['id_ponto'] == id_ponto].copy()
                # --- FIM DA ATUALIZAÇÃO ---

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

                # 4b. Status da Umidade e Inclinômetro (usa o último dado)
                try:
                    for col in ['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc', 'inclinometro_x',
                                'inclinometro_y']:
                        if col not in df_ponto.columns: df_ponto[col] = pd.NA

                    ultimo_dado = df_ponto.sort_values('timestamp').iloc[-1]
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

                risco_geral = max(risco_chuva, risco_umidade, risco_inclinometro_x, risco_inclinometro_y)
                status_geral_texto = STATUS_MAP_HIERARQUICO.get(risco_geral, STATUS_MAP_HIERARQUICO[-1])[0]

                status_atualizado[id_ponto] = {
                    "geral": status_geral_texto,
                    "chuva": status_ponto_chuva,
                    "umidade": status_ponto_umidade,
                    "inclinometro_x": status_ponto_inclinometro_x,
                    "inclinometro_y": status_ponto_inclinometro_y
                }

        status_final_com_alertas = worker_verificar_alertas(status_atualizado, status_antigos_do_disco)

        try:
            with open(data_source.STATUS_FILE, 'w', encoding='utf-8') as f:
                json.dump(status_final_com_alertas, f, indent=2)
        except Exception as e:
            print(f"ERRO CRÍTICO (Thread) ao salvar status: {e}")
            traceback.print_exc()
            data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (Thread) ao salvar status: {e}")

        print(f"WORKER (Thread): Ciclo concluído em {time.time() - inicio_ciclo:.2f}s.")
        return True

    except Exception as e:
        print(f"WORKER ERRO CRÍTICO (Thread) no loop principal: {e}")
        traceback.print_exc()
        data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (Thread loop): {e}")
        return False


# (O resto do arquivo 'background_task_wrapper', 'app.layout' e 'callbacks' permanecem os mesmos)
# ... (código idêntico ao anterior) ...

def background_task_wrapper():
    data_source.setup_disk_paths()
    print("--- Processo Worker (Thread) Iniciado (Modo Sincronizado) ---")
    data_source.adicionar_log("GERAL", "Processo Worker (Thread) iniciado com sucesso.")
    INTERVALO_EM_MINUTOS = 15
    CARENCIA_EM_SEGUNDOS = 60
    while True:
        inicio_total = time.time()
        worker_main_loop()
        tempo_execucao = time.time() - inicio_total
        agora_utc = datetime.datetime.now(datetime.timezone.utc)
        minutos_restantes = INTERVALO_EM_MINUTOS - (agora_utc.minute % INTERVALO_EM_MINUTOS)
        proxima_execucao_base_utc = agora_utc + datetime.timedelta(minutes=minutos_restantes)
        proxima_execucao_base_utc = proxima_execucao_base_utc.replace(
            second=0,
            microsecond=0
        )
        proxima_execucao_com_carencia_utc = proxima_execucao_base_utc + datetime.timedelta(seconds=CARENCIA_EM_SEGUNDOS)
        tempo_para_dormir_seg = (proxima_execucao_com_carencia_utc - agora_utc).total_seconds()
        if tempo_para_dormir_seg < 0:
            print(f"AVISO (Thread): O ciclo demorou {tempo_execucao:.1f}s e perdeu a janela. Rodando novamente...")
            tempo_para_dormir_seg = 1
        print(f"WORKER (Thread): Ciclo levou {tempo_execucao:.1f}s.")
        print(
            f"WORKER (Thread): Próxima execução às {proxima_execucao_com_carencia_utc.isoformat()}. Dormindo por {tempo_para_dormir_seg:.0f}s...")
        time.sleep(tempo_para_dormir_seg)


app.layout = html.Div([
    dcc.Store(id='session-store', data={'logged_in': False, 'user_type': 'guest'}, storage_type='session'),
    dcc.Store(id='store-dados-sessao', storage_type='session'),
    dcc.Store(id='store-ultimo-status', storage_type='session'),
    dcc.Store(id='store-logs-sessao', storage_type='session'),
    dcc.Location(id='url-raiz', refresh=False),
    dcc.Interval(
        id='intervalo-atualizacao-dados',
        interval=10 * 1000,
        n_intervals=0,
        disabled=True
    ),
    html.Div(id='page-container-root')
])


@app.callback(
    Output('page-container-root', 'children'),
    Input('session-store', 'data'),
    Input('url-raiz', 'pathname')
)
def display_page_root(session_data, pathname):
    if session_data and session_data.get('logged_in', False):
        return main_app_page.get_layout()
    else:
        return login_page.get_layout()


@app.callback(
    Output('page-content', 'children'),
    [Input('url-raiz', 'pathname'),
     Input('session-store', 'data')]
)
def display_page_content(pathname, session_data):
    if not session_data.get('logged_in', False):
        return html.Div()
    if pathname.startswith('/ponto/'):
        return specific_dash.get_layout()
    elif pathname == '/dashboard-geral':
        return general_dash.get_layout()
    else:
        return map_view.get_layout()


@app.callback(
    [Output('session-store', 'data'),
     Output('login-error-output', 'children'),
     Output('login-error-output', 'className'),
     Output('input-password', 'value')],
    [Input('btn-login', 'n_clicks'),
     Input('input-password', 'n_submit')],
    State('input-password', 'value'),
    prevent_initial_call=True
)
def login_callback(n_clicks, n_submit, password):
    if not n_clicks and not n_submit:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update
    if not password:
        return dash.no_update, "Por favor, digite a senha.", "text-danger mb-3 text-center", ""
    password = password.strip()
    if password == SENHA_ADMIN:
        new_session = {'logged_in': True, 'user_type': 'admin'}
        return new_session, "", "text-success mb-3 text-center", ""
    elif password == SENHA_CLIENTE:
        new_session = {'logged_in': True, 'user_type': 'client'}
        return new_session, "", "text-success mb-3 text-center", ""
    else:
        return dash.no_update, "Senha incorreta. Tente novamente.", "text-danger mb-3 text-center", ""


@app.callback(
    [Output('session-store', 'data', allow_duplicate=True),
     Output('url-raiz', 'pathname')],
    Input('logout-button', 'n_clicks'),
    prevent_initial_call=True
)
def logout_callback(n_clicks):
    if n_clicks is None or n_clicks == 0:
        return dash.no_update, dash.no_update
    return {'logged_in': False, 'user_type': 'guest'}, '/'


@app.callback(
    Output('intervalo-atualizacao-dados', 'disabled'),
    Input('session-store', 'data')
)
def toggle_interval_update(session_data):
    is_logged_in = session_data and session_data.get('logged_in', False)
    return not is_logged_in


@app.callback(
    [Output('store-dados-sessao', 'data'),
     Output('store-ultimo-status', 'data'),
     Output('store-logs-sessao', 'data')],
    Input('intervalo-atualizacao-dados', 'n_intervals')
)
def update_data_and_logs_from_disk(n_intervals):
    df_completo, status_atual, logs = data_source.get_all_data_from_disk(worker_mode=False)  # Dashboard lê do SQLite
    dados_json_output = df_completo.to_json(date_format='iso', orient='split')
    return dados_json_output, status_atual, logs


data_source.setup_disk_paths()

if not os.environ.get("WERKZEUG_RUN_MAIN"):
    print("Iniciando o worker (coletor de dados) em um thread separado...")
    worker_thread = Thread(target=background_task_wrapper, daemon=True)
    worker_thread.start()
else:
    print("O reloader do Dash está ativo. O worker não será iniciado neste processo.")

if __name__ == '__main__':
    host = '127.0.0.1'
    port = 8050
    print(f"Iniciando o servidor Dash (site) em http://{host}:{port}/")
    try:
        app.run(debug=True, host=host, port=port)
    except Exception as e:
        print(f"ERRO CRÍTICO NA EXECUÇÃO DO APP.RUN: {e}")