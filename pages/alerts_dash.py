# pages/alerts_dash.py
# ATUALIZADO: Layout e títulos restaurados para a versão original (image_b9ee3d.png),
# e estilo de fonte (grande/negrito) restaurado.

import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
from io import StringIO
import traceback
import processamento
from config import PONTOS_DE_ANALISE, RISCO_MAP, STATUS_MAP_HIERARQUICO
from app import app


def criar_card_alerta(titulo, status_texto, valor_texto, cor_bootstrap_class):
    """
    Função helper para criar um card de status padronizado.
    """
    text_color = "text-dark"  # Força texto escuro para contraste

    return dbc.Card(
        [
            # --- Título original do card (Ex: "Chuva (Acumulado 72h)") ---
            dbc.CardHeader(titulo, className="fw-bold text-center"),
            dbc.CardBody(
                [
                    # --- CORREÇÃO: Estilo de fonte grande/negrito restaurado ---
                    html.H3(status_texto, className="card-title fw-bold text-uppercase"),

                    # --- Valor (Ex: "13.6 mm") ---
                    html.H5(valor_texto, className="card-text fw-bold") if valor_texto else None
                ],
                # Centraliza o conteúdo verticalmente e horizontalmente
                className="text-center d-flex flex-column justify-content-center"
            )
        ],
        className=f"{text_color} shadow-sm {cor_bootstrap_class}",  # Aplica a cor (ex: bg-warning)
        style={'height': '100%'}
    )


def get_layout():
    """
    Retorna o layout da página de Dashboard de Alertas.
    (Títulos restaurados para a versão original)
    """
    layout = dbc.Container([
        # --- Título original restaurado ---
        dbc.Row([
            dbc.Col(html.H3("Dashboard de Alertas Individuais"), width=12, className="my-3 text-center")
        ]),

        # --- Sub-título original restaurado ---
        dbc.Row([
            dbc.Col(html.H5("Estação Principal"), width=12, className="mb-3")
        ]),

        # O conteúdo dos cards será carregado aqui
        dbc.Row(
            id='alertas-dash-content',
            children=[dbc.Spinner(size="lg", children="Carregando status...")],
            className="mb-4",
            justify="start"  # Alinhado ao início
        )
    ], fluid=True)

    return layout


@app.callback(
    Output('alertas-dash-content', 'children'),
    [Input('store-dados-sessao', 'data'),
     Input('store-ultimo-status', 'data')]
)
def update_alertas_dashboard(dados_json, status_json):
    """
    Lê os dados da sessão e atualiza os 4 cards de status.
    (Lógica de dados é a mesma, mas os títulos dos cards foram corrigidos)
    """
    if not dados_json or not status_json:
        return dbc.Alert("Aguardando dados da sessão...", color="info")

    try:
        df_completo = pd.read_json(StringIO(dados_json), orient='split')
        status_atual = status_json

        id_ponto = list(PONTOS_DE_ANALISE.keys())[0]

        df_ponto = df_completo[df_completo['id_ponto'] == id_ponto].copy()
        status_ponto_info = status_atual.get(id_ponto, {})

        if df_ponto.empty:
            return dbc.Alert("Sem dados históricos para este ponto.", color="warning")

        # Garante que as colunas numéricas sejam numéricas
        df_ponto['chuva_mm'] = pd.to_numeric(df_ponto['chuva_mm'], errors='coerce').fillna(0.0)
        df_ponto['inclinometro_x'] = pd.to_numeric(df_ponto.get('inclinometro_x'), errors='coerce')
        df_ponto['inclinometro_y'] = pd.to_numeric(df_ponto.get('inclinometro_y'), errors='coerce')

        # --- 1. Card de Chuva (com valor) ---
        status_chuva_txt = status_ponto_info.get("chuva", "SEM DADOS")
        risco_chuva = RISCO_MAP.get(status_chuva_txt, -1)
        cor_chuva_class = STATUS_MAP_HIERARQUICO[risco_chuva][2]  # Pega a classe (ex: 'bg-warning')

        df_chuva_72h = processamento.calcular_acumulado_rolling(df_ponto, horas=72)
        ultima_chuva_72h = df_chuva_72h.iloc[-1]['chuva_mm'] if not df_chuva_72h.empty else 0.0
        valor_chuva_txt = f"{ultima_chuva_72h:.1f} mm"

        # Título original do card
        card_chuva = criar_card_alerta("Chuva (Acumulado 72h)", status_chuva_txt, valor_chuva_txt, cor_chuva_class)

        # --- 2. Card de Umidade (como está) ---
        status_umid_txt = status_ponto_info.get("umidade", "SEM DADOS")
        risco_umid = RISCO_MAP.get(status_umid_txt, -1)
        cor_umid_class = STATUS_MAP_HIERARQUICO[risco_umid][2]
        valor_umid_txt = None  # Sem valor, conforme solicitado

        # Título original do card
        card_umidade = criar_card_alerta("Umidade do Solo", status_umid_txt, valor_umid_txt, cor_umid_class)

        # --- 3. Cards de Inclinômetro (com valor) ---
        df_incli_validos = df_ponto.dropna(subset=['inclinometro_x', 'inclinometro_y']).sort_values('timestamp')
        ultimo_incli_x = df_incli_validos.iloc[-1]['inclinometro_x'] if not df_incli_validos.empty else pd.NA
        ultimo_incli_y = df_incli_validos.iloc[-1]['inclinometro_y'] if not df_incli_validos.empty else pd.NA

        valor_incli_x_txt = f"{ultimo_incli_x:.2f}°" if pd.notna(ultimo_incli_x) else "---"
        valor_incli_y_txt = f"{ultimo_incli_y:.2f}°" if pd.notna(ultimo_incli_y) else "---"

        # Card X
        status_incli_x_txt = status_ponto_info.get("inclinometro_x", "SEM DADOS")
        risco_incli_x = RISCO_MAP.get(status_incli_x_txt, -1)
        cor_incli_x_class = STATUS_MAP_HIERARQUICO[risco_incli_x][2]
        # Título original do card
        card_incli_x = criar_card_alerta("Inclinômetro (Eixo X)", status_incli_x_txt, valor_incli_x_txt,
                                         cor_incli_x_class)

        # Card Y
        status_incli_y_txt = status_ponto_info.get("inclinometro_y", "SEM DADOS")
        risco_incli_y = RISCO_MAP.get(status_incli_y_txt, -1)
        cor_incli_y_class = STATUS_MAP_HIERARQUICO[risco_incli_y][2]
        # Título original do card
        card_incli_y = criar_card_alerta("Inclinômetro (Eixo Y)", status_incli_y_txt, valor_incli_y_txt,
                                         cor_incli_y_class)

        # --- Retorna o layout final com os 4 cards ---
        layout_final = [
            dbc.Col(card_chuva, width=12, md=6, lg=3, className="mb-4"),
            dbc.Col(card_umidade, width=12, md=6, lg=3, className="mb-4"),
            dbc.Col(card_incli_x, width=12, md=6, lg=3, className="mb-4"),
            dbc.Col(card_incli_y, width=12, md=6, lg=3, className="mb-4")
        ]

        return layout_final

    except Exception as e:
        print(f"ERRO no update_alertas_dashboard: {e}")
        traceback.print_exc()
        return dbc.Alert(f"Erro ao gerar cards de alerta: {e}", color="danger")