# index.py (FINAL: Corrigido o NameError da thread e com Rota de Alertas)

import dash
from dash import html, dcc, callback, Input, Output, State
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import pandas as pd
from io import StringIO
import os
import json
import time
import datetime
import traceback

import data_source
import processamento
import alertas
import config  # <--- IMPORTAÇÃO PRINCIPAL

from dotenv import load_dotenv

load_dotenv()

from app import app, server

# --- INÍCIO DA ALTERAÇÃO (Importa o novo dash de alertas) ---
from pages import (
    login as login_page, main_app as main_app_page, map_view,
    general_dash, specific_dash, alerts_dash
)
# --- FIM DA ALTERAÇÃO ---

SENHA_CLIENTE = '@Erione'
SENHA_ADMIN = 'admin456'
DELAY_SEGUNDOS = 60


def atualizar_base_calibracao_db(chave, novo_valor):
    try:
        valor_atual_str = data_source.get_app_state(chave)
        valor_atual = float(valor_atual_str)

        if novo_valor < valor_atual:
            print(f"[Autocalibração DB] Atualizando base '{chave}': de {valor_atual} para {novo_valor}")
            data_source.set_app_state(chave, novo_valor)
            return True
    except Exception as e:
        print(f"ERRO CRÍTICO ao autocalibrar a base no DB: {e}")
    return False


def worker_verificar_alertas(status_novos_dict, status_antigos_dict):
    # --- INÍCIO DA CORREÇÃO (Importa o config que faltava) ---
    from config import PONTOS_DE_ANALISE
    # --- FIM DA CORREÇÃO ---

    default_status = {"geral": "INDEFINIDO", "chuva": "INDEFINIDO", "umidade": "INDEFINIDO",
                      "inclinometro_x": "INDEFINIDO", "inclinometro_y": "INDEFINIDO"}
    if not status_novos_dict: return status_antigos_dict
    if not isinstance(status_antigos_dict, dict) or not all(isinstance(v, dict) for v in status_antigos_dict.values()):
        status_antigos_dict = {pid: PONTOS_DE_ANALISE.keys() for pid in PONTOS_DE_ANALISE.keys()}
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
                alertas.enviar_alerta(id_ponto, nome_ponto, status_novo, status_antigo)
            except Exception as e:
                print(f"Erro ao gerar log/alerta de mudança de status: {e}")
            status_atualizado[id_ponto] = status_novo_info
        else:
            status_atualizado[id_ponto] = status_antigo_info
    return status_atualizado


def on_demand_main_loop():
    # --- INÍCIO DA CORREÇÃO (Importa o config que faltava) ---
    from config import PONTOS_DE_ANALISE, RISCO_MAP, STATUS_MAP_HIERARQUICO
    # --- FIM DA CORREÇÃO ---

    inicio_ciclo = time.time()
    try:
        historico_completo_db, status_antigos_do_disco, logs = data_source.get_all_data_from_disk(worker_mode=False)

        default_status = {"geral": "INDEFINIDO", "chuva": "INDEFINIDO", "umidade": "INDEFINIDO",
                          "inclinometro_x": "INDEFINIDO", "inclinometro_y": "INDEFINIDO"}
        if not status_antigos_do_disco or not isinstance(list(status_antigos_do_disco.values())[0], dict):
            status_antigos_do_disco = {p: default_status for p in PONTOS_DE_ANALISE.keys()}

        print(f"WORKER (On-Demand): Início do ciclo. Histórico (DB) lido: {len(historico_completo_db)} entradas.")

        novos_dados_df, _ = data_source.executar_passo_api_e_salvar(historico_completo_db)

        if not novos_dados_df.empty:
            historico_completo_db, _, _ = data_source.get_all_data_from_disk(worker_mode=False)

        if historico_completo_db.empty:
            status_atualizado = {p: default_status for p in PONTOS_DE_ANALISE.keys()}
        else:
            status_atualizado = {}
            for id_ponto in PONTOS_DE_ANALISE.keys():
                df_ponto = historico_completo_db[historico_completo_db['id_ponto'] == id_ponto].copy()
                if df_ponto.empty:
                    status_atualizado[id_ponto] = default_status
                    continue
                acumulado_72h_df = processamento.calcular_acumulado_rolling(df_ponto, horas=72)
                if not acumulado_72h_df.empty:
                    chuva_72h_final = acumulado_72h_df['chuva_mm'].iloc[-1]
                    status_ponto_chuva, _ = processamento.definir_status_chuva(chuva_72h_final)
                    risco_chuva = RISCO_MAP.get(status_ponto_chuva, -1)
                else:
                    status_ponto_chuva, risco_chuva = "SEM DADOS", -1
                try:
                    for col in ['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc', 'inclinometro_x',
                                'inclinometro_y']:
                        if col not in df_ponto.columns: df_ponto[col] = pd.NA
                    ultimo_dado_valido = df_ponto.sort_values('timestamp').dropna(
                        subset=['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc', 'inclinometro_x',
                                'inclinometro_y'], how='all').iloc[-1]

                    constantes_ponto = {
                        'UMIDADE_BASE_1M': float(data_source.get_app_state('UMIDADE_BASE_1M')),
                        'UMIDADE_BASE_2M': float(data_source.get_app_state('UMIDADE_BASE_2M')),
                        'UMIDADE_BASE_3M': float(data_source.get_app_state('UMIDADE_BASE_3M')),
                        'INCLINOMETRO_BASE_X': float(data_source.get_app_state('INCLINOMETRO_BASE_X')),
                        'INCLINOMETRO_BASE_Y': float(data_source.get_app_state('INCLINOMETRO_BASE_Y')),
                    }

                    umidade_1m = ultimo_dado_valido.get('umidade_1m_perc')
                    if pd.notna(umidade_1m): atualizar_base_calibracao_db("UMIDADE_BASE_1M", umidade_1m)
                    umidade_2m = ultimo_dado_valido.get('umidade_2m_perc')
                    if pd.notna(umidade_2m): atualizar_base_calibracao_db("UMIDADE_BASE_2M", umidade_2m)
                    umidade_3m = ultimo_dado_valido.get('umidade_3m_perc')
                    if pd.notna(umidade_3m): atualizar_base_calibracao_db("UMIDADE_BASE_3M", umidade_3m)

                    constantes_ponto = {
                        'UMIDADE_BASE_1M': float(data_source.get_app_state('UMIDADE_BASE_1M')),
                        'UMIDADE_BASE_2M': float(data_source.get_app_state('UMIDADE_BASE_2M')),
                        'UMIDADE_BASE_3M': float(data_source.get_app_state('UMIDADE_BASE_3M')),
                        'INCLINOMETRO_BASE_X': float(data_source.get_app_state('INCLINOMETRO_BASE_X')),
                        'INCLINOMETRO_BASE_Y': float(data_source.get_app_state('INCLINOMETRO_BASE_Y')),
                    }

                    base_1m, base_2m, base_3m = constantes_ponto['UMIDADE_BASE_1M'], constantes_ponto[
                        'UMIDADE_BASE_2M'], constantes_ponto['UMIDADE_BASE_3M']
                    status_ponto_umidade, _, _ = processamento.definir_status_umidade_hierarquico(umidade_1m,
                                                                                                  umidade_2m,
                                                                                                  umidade_3m, base_1m,
                                                                                                  base_2m, base_3m)
                    risco_umidade = RISCO_MAP.get(status_ponto_umidade, -1)
                    inclinometro_x, inclinometro_y = ultimo_dado_valido.get('inclinometro_x'), ultimo_dado_valido.get(
                        'inclinometro_y')
                    base_x, base_y = constantes_ponto['INCLINOMETRO_BASE_X'], constantes_ponto['INCLINOMETRO_BASE_Y']
                    status_ponto_inclinometro_x, _ = processamento.definir_status_inclinometro_individual(
                        inclinometro_x, base_x)
                    status_ponto_inclinometro_y, _ = processamento.definir_status_inclinometro_individual(
                        inclinometro_y, base_y)
                    risco_inclinometro_x, risco_inclinometro_y = RISCO_MAP.get(status_ponto_inclinometro_x,
                                                                               -1), RISCO_MAP.get(
                        status_ponto_inclinometro_y, -1)
                except Exception as e_umid:
                    print(f"[On-Demand] Erro ao calcular status de umidade/inclinômetro: {e_umid}")
                    status_ponto_umidade, risco_umidade, status_ponto_inclinometro_x, risco_inclinometro_x, status_ponto_inclinometro_y, risco_inclinometro_y = "INDEFINIDO", -1, "INDEFINIDO", -1, "INDEFINIDO", -1
                risco_geral = max(risco_chuva, risco_umidade, risco_inclinometro_x, risco_inclinometro_y)
                status_geral_texto = STATUS_MAP_HIERARQUICO.get(risco_geral, STATUS_MAP_HIERARQUICO[-1])[0]
                status_atualizado[id_ponto] = {"geral": status_geral_texto, "chuva": status_ponto_chuva,
                                               "umidade": status_ponto_umidade,
                                               "inclinometro_x": status_ponto_inclinometro_x,
                                               "inclinometro_y": status_ponto_inclinometro_y}
        status_final_com_alertas = worker_verificar_alertas(status_atualizado, status_antigos_do_disco)

        with open(data_source.STATUS_FILE, 'w', encoding='utf-8') as f:
            json.dump(status_final_com_alertas, f, indent=2)
        print(f"WORKER (On-Demand): Ciclo concluído em {time.time() - inicio_ciclo:.2f}s.")
        return True
    except Exception as e:
        print(f"WORKER (On-Demand) ERRO CRÍTICO no loop principal: {e}")
        traceback.print_exc()
        data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (On-Demand loop): {e}")
        return False


app.layout = html.Div([
    dcc.Store(id='session-store', data={'logged_in': False, 'user_type': 'guest'}, storage_type='session'),
    dcc.Store(id='store-dados-sessao', storage_type='session'),
    dcc.Store(id='store-ultimo-status', storage_type='session'),
    dcc.Store(id='store-logs-sessao', storage_type='session'),
    dcc.Store(id='store-tempo-restante', data={'texto': 'Sincronizando...', 'countdown_s': 0, 'last_sync_s': 0}),
    dcc.Store(id='trigger-atualizacao-dados'),
    dcc.Location(id='url-raiz', refresh=False),
    html.Div(id='dummy-output', style={'display': 'none'}),
    dcc.Interval(id='intervalo-leitura-disco', interval=10 * 1000, n_intervals=0, disabled=True),
    dcc.Store(id='store-intervalo-api-10min', data=10 * 60 * 1000),
    dcc.Interval(id='intervalo-countdown-1s', interval=1000, n_intervals=0, disabled=True),
    html.Div(id='page-container-root')
])


@app.callback(Output('page-container-root', 'children'), Input('session-store', 'data'))
def display_page_root(session_data):
    if session_data and session_data.get('logged_in', False):
        return html.Div([main_app_page.get_navbar(), html.Div(id='page-content')])
    else:
        return login_page.get_layout()


@app.callback(Output('page-content', 'children'), [Input('url-raiz', 'pathname'), Input('session-store', 'data')])
def display_page_content(pathname, session_data):
    if not (session_data and session_data.get('logged_in', False)): return html.Div()

    # --- INÍCIO DA ALTERAÇÃO (Adiciona a nova rota) ---
    if pathname == '/alertas':
        return alerts_dash.get_layout()
    # --- FIM DA ALTERAÇÃO ---

    elif pathname.startswith('/ponto/'):
        return specific_dash.get_layout()
    elif pathname == '/dashboard-geral':
        return general_dash.get_layout()
    else:
        return map_view.get_layout()


@app.callback([Output('session-store', 'data'), Output('login-error-output', 'children'),
               Output('login-error-output', 'className'), Output('input-password', 'value')],
              [Input('btn-login', 'n_clicks'), Input('input-password', 'n_submit')], State('input-password', 'value'),
              prevent_initial_call=True)
def login_callback(n_clicks, n_submit, password):
    if not n_clicks and not n_submit: return dash.no_update, dash.no_update, dash.no_update, dash.no_update
    if not password: return dash.no_update, "Por favor, digite a senha.", "text-danger mb-3 text-center", ""
    password = password.strip()
    if password == SENHA_ADMIN:
        return {'logged_in': True, 'user_type': 'admin'}, "", "text-success mb-3 text-center", ""
    elif password == SENHA_CLIENTE:
        return {'logged_in': True, 'user_type': 'client'}, "", "text-success mb-3 text-center", ""
    else:
        return dash.no_update, "Senha incorreta. Tente novamente.", "text-danger mb-3 text-center", ""


@app.callback([Output('session-store', 'data', allow_duplicate=True), Output('url-raiz', 'pathname')],
              Input('logout-button', 'n_clicks'), prevent_initial_call=True)
def logout_callback(n_clicks):
    if n_clicks is None or n_clicks == 0: return dash.no_update, dash.no_update
    return {'logged_in': False, 'user_type': 'guest'}, '/'


@app.callback([Output('intervalo-leitura-disco', 'disabled'), Output('intervalo-countdown-1s', 'disabled')],
              Input('session-store', 'data'))
def toggle_interval_update(session_data):
    is_logged_in = session_data and session_data.get('logged_in', False)
    return not is_logged_in, not is_logged_in


@app.callback(
    [Output('store-dados-sessao', 'data'),
     Output('store-ultimo-status', 'data'),
     Output('store-logs-sessao', 'data')],
    [Input('intervalo-leitura-disco', 'n_intervals'),
     Input('trigger-atualizacao-dados', 'data')]
)
def update_data_and_logs_from_disk(n_intervals, trigger_data):
    ctx = dash.callback_context
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0] if ctx.triggered else "N/A"
    print(f"[Leitura Disco] Disparado por: {trigger_id}")
    df_completo, status_atual, logs = data_source.get_all_data_from_disk(worker_mode=False)
    dados_json_output = df_completo.to_json(date_format='iso', orient='split')
    return dados_json_output, status_atual, logs


@app.callback(
    [Output('store-intervalo-api-10min', 'data'),
     Output('switch-api-label', 'children'),
     Output('switch-api-label', 'className')],
    Input('switch-api-auto', 'value'),
    prevent_initial_call=True
)
def toggle_api_timer(switch_on):
    current_state_str = data_source.get_app_state("API_AUTO_ATIVADA")
    current_state = current_state_str == "True"

    if current_state == switch_on:
        raise PreventUpdate

    data_source.set_app_state("API_AUTO_ATIVADA", str(switch_on))

    if switch_on:
        print("[API Timer] Ligado. Próxima busca em 10 min.")
        return 10 * 60 * 1000, "ON", "ms-2 bg-light-green"
    else:
        print("[API Timer] Desligado.")
        return 0, "OFF", "ms-2"


def get_proxima_execucao():
    INTERVALO_MINUTOS = 10
    ATRASO_ADICIONAL_SEGUNDOS = DELAY_SEGUNDOS
    AGORA_UTC = datetime.datetime.now(datetime.timezone.utc)
    minuto_atual = AGORA_UTC.minute
    minuto_proximo_ciclo = (minuto_atual // INTERVALO_MINUTOS + 1) * INTERVALO_MINUTOS
    proxima_execucao_base = AGORA_UTC.replace(second=0, microsecond=0)
    if minuto_proximo_ciclo >= 60:
        proxima_execucao_base = proxima_execucao_base + datetime.timedelta(hours=1)
        minuto_proximo_ciclo = 0
    proxima_execucao_base = proxima_execucao_base.replace(minute=minuto_proximo_ciclo)
    proxima_execucao_final = proxima_execucao_base + datetime.timedelta(seconds=ATRASO_ADICIONAL_SEGUNDOS)
    while proxima_execucao_final <= AGORA_UTC:
        proxima_execucao_base += datetime.timedelta(minutes=INTERVALO_MINUTOS)
        proxima_execucao_final = proxima_execucao_base + datetime.timedelta(seconds=ATRASO_ADICIONAL_SEGUNDOS)
    return proxima_execucao_final


@app.callback([Output('store-tempo-restante', 'data', allow_duplicate=True),
               Output('dummy-output', 'children', allow_duplicate=True)],
              Input('intervalo-countdown-1s', 'n_intervals'), [State('store-tempo-restante', 'data')],
              prevent_initial_call=True)
def update_sync_time(n_intervals, current_store_data):
    api_auto_ativada_global = data_source.get_app_state("API_AUTO_ATIVADA") == "True"

    if not api_auto_ativada_global:
        return {'texto': "API Desligada (Global)", 'countdown_s': -1, 'last_sync_s': 0}, ""

    AGORA_UTC = datetime.datetime.now(datetime.timezone.utc)
    current_s = current_store_data.get('countdown_s', 0)

    if current_s <= 0:
        print("[Timer Sync] Autocorrigindo/Reiniciando. Recalculando tempo...")
        proxima_execucao = get_proxima_execucao()
        tempo_restante_s = max(0, int((proxima_execucao - AGORA_UTC).total_seconds()))
        min_restantes, sec_restantes = tempo_restante_s // 60, tempo_restante_s % 60
        texto_tempo = f"Próxima Requisição: {min_restantes:02d}:{sec_restantes:02d}"
        return {'texto': texto_tempo, 'countdown_s': tempo_restante_s, 'last_sync_s': time.time()}, ""

    if current_s > 1:
        next_s = current_s - 1
        min_restantes, sec_restantes = next_s // 60, next_s % 60
        texto_tempo = f"Próxima Requisição: {min_restantes:02d}:{sec_restantes:02d}"
        return {'texto': texto_tempo, 'countdown_s': next_s, 'last_sync_s': time.time()}, ""
    elif current_s == 1:
        print("[Timer Sync] ALVO ATINGIDO (1s). Disparando requisição da API.")
        return {'texto': "REQUISITANDO...", 'countdown_s': -2, 'last_sync_s': time.time()}, ""

    return dash.no_update, dash.no_update


@app.callback(Output('trigger-atualizacao-dados', 'data'), Input('store-tempo-restante', 'data'),
              prevent_initial_call=True)
def callback_disparador_api(data_store):
    if data_store and data_store.get('countdown_s') == -2:
        api_auto_ativada_global = data_source.get_app_state("API_AUTO_ATIVADA") == "True"

        if not api_auto_ativada_global:
            print("[API Timer] Tentativa de disparo, mas API desativada globalmente. Abortando.")
            return dash.no_update
        print(f"[API Timer] Disparado por Store. Iniciando busca em background...")
        try:
            sucesso = on_demand_main_loop()
            if not sucesso: data_source.adicionar_log("GERAL", "AVISO: A busca automática (10min) falhou.")
        except Exception as e:
            print(f"ERRO CRÍTICO [API Timer]: {e}")
            traceback.print_exc()
            data_source.adicionar_log("GERAL", f"ERRO CRÍTICO (API Timer): {e}")
            return dash.no_update
        ping_data = str(datetime.datetime.now())
        print(f"[API Timer] Busca concluída. Pingando o 'trigger-atualizacao-dados'.")
        return ping_data
    return dash.no_update


@app.callback(Output('tempo-restante-sincronia', 'children'), Input('store-tempo-restante', 'data'))
def display_sync_time(data):
    if data and data.get('texto'): return data['texto']
    return "Sincronizando..."


if __name__ == '__main__':
    data_source.setup_disk_paths()

    host = '127.0.0.1'
    port = 8050
    print(f"Iniciando o servidor Dash (WEB) em http://{host}:{port}/")
    try:
        app.run(debug=True, host=host, port=port, use_reloader=False)
    except Exception as e:
        print(f"ERRO CRÍTICO NA EXECUÇÃO DO APP.RUN: {e}")