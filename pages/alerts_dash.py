# pages/alerts_dash.py (ATUALIZADO: Removido "(Hierárquico)" do Card)
# Esta versão cria cards individuais para cada sensor.

import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import json

from app import app
from config import (
    PONTOS_DE_ANALISE,
    RISCO_MAP,
    STATUS_MAP_HIERARQUICO
)


def get_layout():
    """ Retorna o layout da página de Alertas. """
    return dbc.Container([
        html.H3("Dashboard de Alertas Individuais", className="text-center my-4",
                style={'color': '#000000', 'font-weight': 'bold'}),

        # O conteúdo será gerado por PONTO DE ANÁLISE
        html.Div(id='alerts-dash-content', children=[
            dbc.Spinner(size="lg", children="Carregando status...")
        ]),

    ], fluid=True)


@app.callback(
    Output('alerts-dash-content', 'children'),
    Input('store-ultimo-status', 'data')
)
def update_alerts_dashboard(status_json):
    if not status_json:
        return dbc.Alert("Aguardando dados de status...", color="info")

    status_dict = status_json
    layout_final = []

    # Status padrão para caso um ponto não seja encontrado
    default_status = {
        "geral": "SEM DADOS", "chuva": "SEM DADOS", "umidade": "SEM DADOS",
        "inclinometro_x": "SEM DADOS", "inclinometro_y": "SEM DADOS"
    }

    # Itera sobre todos os pontos definidos no config.py (ex: "Ponto-1")
    for id_ponto, config in PONTOS_DE_ANALISE.items():
        status_info = status_dict.get(id_ponto, default_status)
        cards_ponto = []  # Lista para os 4 cards deste ponto

        # --- 1. Card de Chuva ---
        status_chuva = status_info.get("chuva", "SEM DADOS")
        risco_chuva = RISCO_MAP.get(status_chuva, -1)
        texto_chuva, _, bg_class_chuva = STATUS_MAP_HIERARQUICO.get(risco_chuva, STATUS_MAP_HIERARQUICO[-1])

        card_chuva = dbc.Card(
            [
                dbc.CardHeader("Chuva (Acumulado 72h)"),
                dbc.CardBody(
                    [html.H5(texto_chuva, className="card-title", style={'font-weight': 'bold'})]
                )
            ],
            className=f"shadow h-100 {bg_class_chuva}",
        )
        cards_ponto.append(dbc.Col(card_chuva, width=12, md=6, lg=3, className="mb-4"))

        # --- 2. Card de Umidade ---
        status_umidade = status_info.get("umidade", "SEM DADOS")
        risco_umidade = RISCO_MAP.get(status_umidade, -1)
        texto_umidade, _, bg_class_umidade = STATUS_MAP_HIERARQUICO.get(risco_umidade, STATUS_MAP_HIERARQUICO[-1])

        card_umidade = dbc.Card(
            [
                # --- ALTERAÇÃO AQUI ---
                dbc.CardHeader("Umidade do Solo"),
                # --- FIM DA ALTERAÇÃO ---
                dbc.CardBody(
                    [html.H5(texto_umidade, className="card-title", style={'font-weight': 'bold'})]
                )
            ],
            className=f"shadow h-100 {bg_class_umidade}",
        )
        cards_ponto.append(dbc.Col(card_umidade, width=12, md=6, lg=3, className="mb-4"))

        # --- 3. Card de Inclinômetro X ---
        status_incli_x = status_info.get("inclinometro_x", "SEM DADOS")
        risco_incli_x = RISCO_MAP.get(status_incli_x, -1)
        texto_incli_x, _, bg_class_incli_x = STATUS_MAP_HIERARQUICO.get(risco_incli_x, STATUS_MAP_HIERARQUICO[-1])

        card_incli_x = dbc.Card(
            [
                dbc.CardHeader("Inclinômetro (Eixo X)"),
                dbc.CardBody(
                    [html.H5(texto_incli_x, className="card-title", style={'font-weight': 'bold'})]
                )
            ],
            className=f"shadow h-100 {bg_class_incli_x}",
        )
        cards_ponto.append(dbc.Col(card_incli_x, width=12, md=6, lg=3, className="mb-4"))

        # --- 4. Card de Inclinômetro Y ---
        status_incli_y = status_info.get("inclinometro_y", "SEM DADOS")
        risco_incli_y = RISCO_MAP.get(status_incli_y, -1)
        texto_incli_y, _, bg_class_incli_y = STATUS_MAP_HIERARQUICO.get(risco_incli_y, STATUS_MAP_HIERARQUICO[-1])

        card_incli_y = dbc.Card(
            [
                dbc.CardHeader("Inclinômetro (Eixo Y)"),
                dbc.CardBody(
                    [html.H5(texto_incli_y, className="card-title", style={'font-weight': 'bold'})]
                )
            ],
            className=f"shadow h-100 {bg_class_incli_y}",
        )
        cards_ponto.append(dbc.Col(card_incli_y, width=12, md=6, lg=3, className="mb-4"))

        # Adiciona o título da Estação e seus 4 cards ao layout
        layout_final.append(html.H4(config['nome'], className="mt-4"))
        layout_final.append(dbc.Row(cards_ponto))
        layout_final.append(html.Hr())

    if not layout_final:
        return dbc.Alert("Nenhum ponto de análise encontrado.", color="warning")

    return layout_final