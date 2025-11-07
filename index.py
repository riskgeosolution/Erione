# index.py (CORRIGIDO: Lógica do Worker REMOVIDA)
# Este arquivo agora é APENAS o servidor web (Dash).

import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
from io import StringIO
import os
import json
from dotenv import load_dotenv

# --- Imports de Thread e Worker REMOVIDOS ---

load_dotenv()

from app import app, server

from pages import login as login_page
from pages import main_app as main_app_page
from pages import map_view, general_dash, specific_dash
import data_source
import config

# --- Imports de Processamento e Alertas REMOVIDOS (não são mais usados aqui) ---


# --- SENHA DO APP (LOGIN) ---
SENHA_CLIENTE = '123'
SENHA_ADMIN = 'admin456'

# ==============================================================================
# --- LÓGICA DO WORKER (MOVIDA PARA worker.py) ---
# ==============================================================================
# (Todo o código do worker_main_loop, worker_verificar_alertas,
# e background_task_wrapper foi removido daqui)
# ==============================================================================


# --- Layout Principal do Dash ---
app.layout = html.Div([
    dcc.Store(id='session-store', data={'logged_in': False, 'user_type': 'guest'}, storage_type='session'),
    dcc.Store(id='store-dados-sessao', storage_type='session'),
    dcc.Store(id='store-ultimo-status', storage_type='session'),
    dcc.Store(id='store-logs-sessao', storage_type='session'),
    dcc.Location(id='url-raiz', refresh=False),
    dcc.Interval(
        id='intervalo-atualizacao-dados',
        interval=10 * 1000,  # Atualiza os dados do site a cada 10 segundos
        n_intervals=0,
        disabled=True  # Habilitado somente após o login
    ),
    html.Div(id='page-container-root')
])


# ==============================================================================
# --- CALLBACKS DO DASH (Interface) ---
# ==============================================================================

@app.callback(
    Output('page-container-root', 'children'),
    Input('session-store', 'data'),
    Input('url-raiz', 'pathname')
)
def display_page_root(session_data, pathname):
    """ Exibe a página de login OU o app principal """
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
    """ Roteia o conteúdo da página (Mapa, Dashboard Geral, Dashboard Específico) """
    if not session_data.get('logged_in', False):
        return html.Div()
    if pathname.startswith('/ponto/'):
        return specific_dash.get_layout()
    elif pathname == '/dashboard-geral':
        return general_dash.get_layout()
    else:
        # A página padrão (/) é o mapa
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
    """ Valida a senha """
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
    """ Faz o logout """
    if n_clicks is None or n_clicks == 0:
        return dash.no_update, dash.no_update
    return {'logged_in': False, 'user_type': 'guest'}, '/'


@app.callback(
    Output('intervalo-atualizacao-dados', 'disabled'),
    Input('session-store', 'data')
)
def toggle_interval_update(session_data):
    """ Ativa o 'refresh' de dados (intervalo) somente se estiver logado """
    is_logged_in = session_data and session_data.get('logged_in', False)
    return not is_logged_in


@app.callback(
    [Output('store-dados-sessao', 'data'),
     Output('store-ultimo-status', 'data'),
     Output('store-logs-sessao', 'data')],
    Input('intervalo-atualizacao-dados', 'n_intervals')
)
def update_data_and_logs_from_disk(n_intervals):
    """
    Este callback é o 'coração' do frontend.
    A cada 10 segundos, ele lê os dados (SQLite) e o status (JSON)
    que o worker.py preparou.
    """
    df_completo, status_atual, logs = data_source.get_all_data_from_disk(worker_mode=False)  # Dashboard lê do SQLite
    dados_json_output = df_completo.to_json(date_format='iso', orient='split')
    return dados_json_output, status_atual, logs


# --- Ponto de entrada do Servidor WEB ---
if __name__ == '__main__':
    # Garante que o servidor web saiba onde encontrar os arquivos de dados
    data_source.setup_disk_paths()

    host = '127.0.0.1'
    port = 8050
    print(f"Iniciando o servidor Dash (WEB) em http://{host}:{port}/")
    try:
        app.run(debug=True, host=host, port=port)
    except Exception as e:
        print(f"ERRO CRÍTICO NA EXECUÇÃO DO APP.RUN: {e}")