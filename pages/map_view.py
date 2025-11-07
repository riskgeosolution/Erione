# pages/map_view.py (CORRIGIDO: Remove "Estação" duplicada do card e dos pinos)

import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import dash_leaflet as dl
import pandas as pd
from io import StringIO
import traceback
import numpy as np
import json

from app import app
from config import PONTOS_DE_ANALISE, CONSTANTES_PADRAO, RISCO_MAP, STATUS_MAP_HIERARQUICO, CHUVA_LIMITE_VERDE, \
    CHUVA_LIMITE_AMARELO, CHUVA_LIMITE_LARANJA
import processamento
import data_source


def get_layout():
    """ Retorna o layout da página do mapa (1 Card Superior). """
    print("Executando map_view.get_layout() (1 Card Superior)")
    try:
        primeiro_ponto_id = list(PONTOS_DE_ANALISE.keys())[0]
        map_center = PONTOS_DE_ANALISE[primeiro_ponto_id]['lat_lon']

        layout = dbc.Container([
            dbc.Row([dbc.Col(
                html.Div([
                    dl.Map(
                        id='mapa-principal', center=map_center, zoom=15,
                        touchZoom=True,
                        children=[
                            dl.TileLayer(),
                            dl.LayerGroup(id='map-pins-layer'),
                            dbc.Card(
                                [dbc.CardHeader("Resumo da Estação", className="text-center small py-1"),
                                 dbc.CardBody(id='map-summary-card-content', children=[dbc.Spinner(size="sm")])],
                                className="map-summary-card map-summary-left", style={"width": "250px"}
                            )
                        ],
                        style={'width': '100%', 'height': '80vh', 'min-height': '600px'}
                    ),
                ], style={'position': 'relative'}),
                width=12, className="mb-4")])
        ], fluid=True)
        print("Layout do mapa (1 Card) criado com sucesso.")
        return layout
    except Exception as e:
        print(f"ERRO CRÍTICO em map_view.get_layout: {e}");
        return html.Div([html.H1("Erro Layout Mapa"), html.Pre(traceback.format_exc())])


@app.callback(
    Output('map-pins-layer', 'children'),
    Input('store-dados-sessao', 'data')
)
def update_map_pins(dados_json):
    if not dados_json:
        return []
    try:
        df_completo = pd.read_json(StringIO(dados_json), orient='split').copy()
        if 'timestamp' not in df_completo.columns or df_completo.empty:
            return []
        df_completo.loc[:, 'timestamp'] = pd.to_datetime(df_completo['timestamp'], errors='coerce')
        df_validos = df_completo.dropna(subset=['timestamp']).copy()
        if df_validos.empty:
            return []
        ultimo_timestamp_geral = df_validos['timestamp'].max()
        if pd.isna(ultimo_timestamp_geral):
            return []
    except Exception as e:
        print(f"ERRO CRÍTICO em update_map_pins ao processar dados: {e}")
        return []

    pinos_do_mapa = []
    df_acumulado_completo = processamento.calcular_acumulado_rolling(df_completo, horas=72)
    df_acumulado_ultimo = df_acumulado_completo.groupby('id_ponto').last().reset_index()

    for id_ponto, config in PONTOS_DE_ANALISE.items():
        dados_ponto_acumulado = df_acumulado_ultimo[df_acumulado_ultimo['id_ponto'] == id_ponto]
        chuva_72h_pino = 0.0
        if not dados_ponto_acumulado.empty:
            chuva_72h_pino = dados_ponto_acumulado.iloc[0].get('chuva_mm', 0.0)
            if pd.isna(chuva_72h_pino):
                chuva_72h_pino = 0.0

        # --- INÍCIO DA CORREÇÃO (Popup e Tooltip) ---
        pino = dl.Marker(
            position=config['lat_lon'],
            children=[
                dl.Tooltip(config['nome']),  # <-- CORRIGIDO
                dl.Popup([
                    html.H5(config['nome']),  # <-- CORRIGIDO
                    html.P(f"Chuva (72h): {chuva_72h_pino:.1f} mm"),
                    dbc.Button("Ver Dashboard", href=f"/ponto/{id_ponto}", size="sm", color="primary")
                ])
            ]
        )
        # --- FIM DA CORREÇÃO ---

        pinos_do_mapa.append(pino)
    print(f"[map_view] update_map_pins: Gerados {len(pinos_do_mapa)} pinos.")
    return pinos_do_mapa


# --- Função create_km_block (ATUALIZADA) ---
def create_km_block(id_ponto, config, df_ponto, status_ponto_info):
    """
    Cria o bloco de resumo com 4 cards (2x2): Chuva, Umidade, Incli X, Incli Y.
    """

    # 1. Extrair Status (do worker)
    status_chuva_txt = status_ponto_info.get("chuva", "SEM DADOS")
    status_umid_txt = status_ponto_info.get("umidade", "SEM DADOS")
    status_incli_x_txt = status_ponto_info.get("inclinometro_x", "SEM DADOS")
    status_incli_y_txt = status_ponto_info.get("inclinometro_y", "SEM DADOS")

    risco_chuva = RISCO_MAP.get(status_chuva_txt, -1)
    _, status_chuva_col, cor_chuva_class = STATUS_MAP_HIERARQUICO.get(risco_chuva, STATUS_MAP_HIERARQUICO[-1])

    risco_umidade = RISCO_MAP.get(status_umid_txt, -1)
    _, status_umid_col, cor_umidade_class = STATUS_MAP_HIERARQUICO.get(risco_umidade, STATUS_MAP_HIERARQUICO[-1])

    risco_incli_x = RISCO_MAP.get(status_incli_x_txt, -1)
    _, status_incli_col_x, cor_incli_class_x = STATUS_MAP_HIERARQUICO.get(risco_incli_x, STATUS_MAP_HIERARQUICO[-1])

    risco_incli_y = RISCO_MAP.get(status_incli_y_txt, -1)
    _, status_incli_col_y, cor_incli_class_y = STATUS_MAP_HIERARQUICO.get(risco_incli_y, STATUS_MAP_HIERARQUICO[-1])

    # 2. Extrair Valores (do dataframe)
    ultima_chuva_72h = 0.0
    ultimo_incli_x = 0.0
    ultimo_incli_y = 0.0

    try:
        if not df_ponto.empty:
            if 'timestamp' in df_ponto.columns:
                df_ponto.loc[:, 'timestamp'] = pd.to_datetime(df_ponto['timestamp'], errors='coerce')
                if df_ponto['timestamp'].dt.tz is None:
                    df_ponto.loc[:, 'timestamp'] = df_ponto['timestamp'].dt.tz_localize('UTC')
                df_ponto = df_ponto.dropna(subset=['timestamp']).copy()

            if 'chuva_mm' in df_ponto.columns:
                df_ponto.loc[:, 'chuva_mm'] = pd.to_numeric(df_ponto['chuva_mm'], errors='coerce')
            df_chuva_72h = processamento.calcular_acumulado_rolling(df_ponto, horas=72)
            if not df_chuva_72h.empty:
                chuva_val = df_chuva_72h.iloc[-1]['chuva_mm']
                if not pd.isna(chuva_val):
                    ultima_chuva_72h = chuva_val

            if 'inclinometro_x' in df_ponto.columns and 'inclinometro_y' in df_ponto.columns:
                df_ponto.loc[:, 'inclinometro_x'] = pd.to_numeric(df_ponto['inclinometro_x'], errors='coerce')
                df_ponto.loc[:, 'inclinometro_y'] = pd.to_numeric(df_ponto['inclinometro_y'], errors='coerce')

                df_incli_validos = df_ponto.dropna(subset=['inclinometro_x', 'inclinometro_y']).sort_values('timestamp')
                if not df_incli_validos.empty:
                    ultimo_dado_incli = df_incli_validos.iloc[-1]
                    ultimo_incli_x = ultimo_dado_incli['inclinometro_x']
                    ultimo_incli_y = ultimo_dado_incli['inclinometro_y']

    except Exception as e:
        print(f"ERRO GERAL em create_km_block para {id_ponto}: {e}")
        traceback.print_exc()
        ultima_chuva_72h = 0.0
        status_chuva_txt = "ERRO";
        status_chuva_col = "danger";
        cor_chuva_class = "bg-danger"

    # 3. Lógica dos Gauges (Visuais)
    chuva_max_visual = 90.0
    chuva_percent = max(0, min(100, (ultima_chuva_72h / chuva_max_visual) * 100))
    if status_chuva_txt == "SEM DADOS":
        chuva_percent = 0

    umidade_percent_realista = 0
    if risco_umidade == 0:
        umidade_percent_realista = 25
    elif risco_umidade == 1:
        umidade_percent_realista = 50
    elif risco_umidade == 2:
        umidade_percent_realista = 75
    elif risco_umidade == 3:
        umidade_percent_realista = 100

    incli_percent_x = 0
    if risco_incli_x == 0:
        incli_percent_x = 25
    elif risco_incli_x == 1:
        incli_percent_x = 50
    elif risco_incli_x == 2:
        incli_percent_x = 75
    elif risco_incli_x == 3:
        incli_percent_x = 100

    incli_percent_y = 0
    if risco_incli_y == 0:
        incli_percent_y = 25
    elif risco_incli_y == 1:
        incli_percent_y = 50
    elif risco_incli_y == 2:
        incli_percent_y = 75
    elif risco_incli_y == 3:
        incli_percent_y = 100

    # 4. Montagem dos Gauges
    chuva_gauge = html.Div(
        [
            html.Div(className=f"gauge-bar {cor_chuva_class}", style={'height': f'{chuva_percent}%'}),
            html.Div(
                [html.Span(f"{ultima_chuva_72h:.0f}"), html.Br(), html.Span("mm", style={'fontSize': '0.8em'})],
                className="gauge-label", style={'fontSize': '2.0em', 'lineHeight': '1.1'}
            )
        ], className="gauge-vertical-container"
    )
    umidade_gauge = html.Div(
        [
            html.Div(className=f"gauge-bar {cor_umidade_class}", style={'height': f'{umidade_percent_realista}%'})
        ],
        className="gauge-vertical-container"
    )
    inclinometro_gauge_x = html.Div(
        [
            html.Div(className=f"gauge-bar {cor_incli_class_x}", style={'height': f'{incli_percent_x}%'}),
            html.Div(
                [html.Span(f"{ultimo_incli_x:.1f}°", style={'fontSize': '1.0em'})],
                className="gauge-label", style={'lineHeight': '1.1'}
            )
        ], className="gauge-vertical-container"
    )
    inclinometro_gauge_y = html.Div(
        [
            html.Div(className=f"gauge-bar {cor_incli_class_y}", style={'height': f'{incli_percent_y}%'}),
            html.Div(
                [html.Span(f"{ultimo_incli_y:.1f}°", style={'fontSize': '1.0em'})],
                className="gauge-label", style={'lineHeight': '1.1'}
            )
        ], className="gauge-vertical-container"
    )

    chuva_badge = dbc.Badge(status_chuva_txt, color=status_chuva_col, className="w-100 mt-1 small badge-black-text")
    umidade_badge = dbc.Badge(status_umid_txt, color=status_umid_col, className="w-100 mt-1 small badge-black-text")
    incli_badge_x = dbc.Badge(status_incli_x_txt, color=status_incli_col_x,
                              className="w-100 mt-1 small badge-black-text")
    incli_badge_y = dbc.Badge(status_incli_y_txt, color=status_incli_col_y,
                              className="w-100 mt-1 small badge-black-text")

    link_destino = f"/ponto/{id_ponto}"

    # --- INÍCIO DA CORREÇÃO (Título do Card) ---
    conteudo_bloco = html.Div([
        html.H6(config['nome'], className="text-center mb-2"),  # <-- CORRIGIDO
        dbc.Row([
            dbc.Col([html.Div("Chuva (72h)", className="small text-center"), chuva_gauge, chuva_badge], width=6,
                    className="mb-2"),
            dbc.Col([html.Div("Umid. Solo", className="small text-center"), umidade_gauge, umidade_badge], width=6,
                    className="mb-2"),
        ], className="g-2"),
        dbc.Row([
            dbc.Col([html.Div("Inclinômetro X", className="small text-center"), inclinometro_gauge_x, incli_badge_x],
                    width=6),
            dbc.Col([html.Div("Inclinômetro Y", className="small text-center"), inclinometro_gauge_y, incli_badge_y],
                    width=6),
        ], className="g-2"),
    ], className="km-summary-block")
    # --- FIM DA CORREÇÃO ---

    return html.A(
        conteudo_bloco,
        href=link_destino,
        style={'textDecoration': 'none', 'color': 'inherit'}
    )


# --- Callback do Card (ATUALIZADO) ---
@app.callback(
    Output('map-summary-card-content', 'children'),
    [Input('store-dados-sessao', 'data'),
     Input('store-ultimo-status', 'data')]
)
def update_summary_card(dados_json, status_json):
    if not dados_json or not status_json:
        return dbc.Spinner(size="sm")
    try:
        df_completo = pd.read_json(StringIO(dados_json), orient='split').copy()
        if df_completo.empty or 'id_ponto' not in df_completo.columns:
            return dbc.Alert("Dados indisponíveis.", color="warning", className="m-2 small")

        df_completo.loc[:, 'timestamp'] = pd.to_datetime(df_completo['timestamp'], errors='coerce')
        df_completo.loc[:, 'chuva_mm'] = pd.to_numeric(df_completo['chuva_mm'], errors='coerce')
        if df_completo['timestamp'].dt.tz is None:
            df_completo.loc[:, 'timestamp'] = df_completo['timestamp'].dt.tz_localize('UTC')
        df_completo = df_completo.dropna(subset=['timestamp']).copy()

        status_atual = status_json

        id_ponto = list(PONTOS_DE_ANALISE.keys())[0]
        config = PONTOS_DE_ANALISE[id_ponto]

        df_ponto = df_completo[df_completo['id_ponto'] == id_ponto].copy()

        status_ponto_info = status_atual.get(id_ponto, {
            "geral": "SEM DADOS", "chuva": "SEM DADOS", "umidade": "SEM DADOS",
            "inclinometro_x": "SEM DADOS", "inclinometro_y": "SEM DADOS"
        })

        km_block = create_km_block(id_ponto, config, df_ponto, status_ponto_info)

        return [km_block]

    except Exception as e:
        print(f"ERRO GERAL em update_summary_card: {e}")
        traceback.print_exc()
        return dbc.Alert(f"Erro ao carregar dados (Card): {e}", color="danger", className="m-2 small")