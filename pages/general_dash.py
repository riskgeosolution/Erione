# pages/general_dash.py (ATUALIZADO para Inclinômetros)

import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from io import StringIO
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from app import app, TEMPLATE_GRAFICO_MODERNO
from config import PONTOS_DE_ANALISE, FREQUENCIA_API_SEGUNDOS
import processamento

CORES_UMIDADE = {
    '1m': 'green',
    '2m': '#FFD700',
    '3m': 'red'
}

# --- INÍCIO DA ATUALIZAÇÃO (Nomes dos Inclinômetros) ---
CORES_INCLINOMETRO = {
    'Inclinômetro X': '#007BFF',  # Azul
    'Inclinômetro Y': '#E83E8C'  # Rosa
}


# --- FIM DA ATUALIZAÇÃO ---


def get_layout():
    opcoes_tempo_lista = [1, 3, 6, 12, 18, 24, 48, 72, 84, 96]
    opcoes_tempo = [{'label': f'Últimas {h} horas', 'value': h} for h in opcoes_tempo_lista] + [
        {'label': 'Todo o Histórico', 'value': 14 * 24}]

    return dbc.Container([
        dbc.Row([
            dbc.Col(dbc.Label("Período (Gráficos):"), width="auto"),
            dbc.Col(
                dcc.Dropdown(
                    id='general-graph-time-selector',
                    options=opcoes_tempo,
                    value=72,
                    clearable=False,
                    searchable=False
                ),
                width=12, lg=4
            )
        ], align="center", className="my-3"),
        html.Div(id='general-dash-content', children=[dbc.Spinner(size="lg", children="Carregando...")])
    ], fluid=True)


@app.callback(
    Output('general-dash-content', 'children'),
    Input('store-dados-sessao', 'data'),
    Input('general-graph-time-selector', 'value')
)
def update_general_dashboard(dados_json, selected_hours):
    if not dados_json or selected_hours is None:
        return dbc.Spinner(size="lg", children="Carregando dados...")
    try:
        df_completo = pd.read_json(StringIO(dados_json), orient='split')

        df_completo['timestamp'] = pd.to_datetime(df_completo['timestamp'])
        if df_completo['timestamp'].dt.tz is None:
            df_completo['timestamp'] = df_completo['timestamp'].dt.tz_localize('UTC')
        else:
            df_completo['timestamp'] = df_completo['timestamp'].dt.tz_convert('UTC')

        df_completo['timestamp_local'] = df_completo['timestamp'].dt.tz_convert('America/Sao_Paulo')

        # --- INÍCIO DA ATUALIZAÇÃO (Converte novas colunas) ---
        df_completo['chuva_mm'] = pd.to_numeric(df_completo['chuva_mm'], errors='coerce')
        df_completo['umidade_1m_perc'] = pd.to_numeric(df_completo['umidade_1m_perc'], errors='coerce')
        df_completo['umidade_2m_perc'] = pd.to_numeric(df_completo['umidade_2m_perc'], errors='coerce')
        df_completo['umidade_3m_perc'] = pd.to_numeric(df_completo['umidade_3m_perc'], errors='coerce')
        df_completo['inclinometro_x'] = pd.to_numeric(df_completo.get('inclinometro_x'), errors='coerce')
        df_completo['inclinometro_y'] = pd.to_numeric(df_completo.get('inclinometro_y'), errors='coerce')
        # --- FIM DA ATUALIZAÇÃO ---

    except Exception as e:
        return dbc.Alert(f"Erro ao ler dados: {e}", color="danger")

    layout_geral = []
    for id_ponto, config in PONTOS_DE_ANALISE.items():
        df_ponto = df_completo[df_completo['id_ponto'] == id_ponto].copy()
        if df_ponto.empty: continue

        ultimo_timestamp_no_df = df_ponto['timestamp_local'].max()
        limite_tempo = ultimo_timestamp_no_df - pd.Timedelta(hours=selected_hours)
        df_ponto_plot = df_ponto[df_ponto['timestamp_local'] >= limite_tempo].copy()
        n_horas_titulo = selected_hours

        # (Lógica da Chuva - Mantida)
        df_chuva_acumulada_completo = processamento.calcular_acumulado_rolling(df_ponto, horas=selected_hours)
        df_chuva_acumulada_plot = df_chuva_acumulada_completo[
            df_chuva_acumulada_completo['timestamp'] >= df_ponto_plot['timestamp'].min()
            ].copy()
        if 'timestamp' in df_chuva_acumulada_plot.columns:
            if df_chuva_acumulada_plot['timestamp'].dt.tz is None:
                df_chuva_acumulada_plot.loc[:, 'timestamp'] = df_chuva_acumulada_plot['timestamp'].dt.tz_localize('UTC')
            df_chuva_acumulada_plot.loc[:, 'timestamp_local'] = df_chuva_acumulada_plot['timestamp'].dt.tz_convert(
                'America/Sao_Paulo')
        else:
            df_chuva_acumulada_plot.loc[:, 'timestamp_local'] = df_chuva_acumulada_plot['timestamp']

        fig_chuva = make_subplots(specs=[[{"secondary_y": True}]])
        fig_chuva.add_trace(
            go.Bar(x=df_ponto_plot['timestamp_local'], y=df_ponto_plot['chuva_mm'], name='Pluv. Horária',
                   marker_color='#2C3E50', opacity=0.8), secondary_y=False)
        fig_chuva.add_trace(
            go.Scatter(x=df_chuva_acumulada_plot['timestamp_local'], y=df_chuva_acumulada_plot['chuva_mm'],
                       name=f'Acumulada ({selected_hours}h)',
                       mode='lines', line=dict(color='#007BFF', width=2.5)), secondary_y=True)
        fig_chuva.update_layout(
            title_text=f"Pluviometria - Estação {config['nome']} ({n_horas_titulo}h)",
            template=TEMPLATE_GRAFICO_MODERNO,
            margin=dict(l=40, r=20, t=50, b=80),
            legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor='center', x=0.5),
            xaxis_title="Data e Hora",
            yaxis_title="Pluv. Horária (mm)",
            yaxis2_title=f"Acumulada ({selected_hours}h)",
            hovermode="x unified", bargap=0.1)
        fig_chuva.update_xaxes(
            dtick=3 * 60 * 60 * 1000,
            tickformat="%d/%m %Hh",
            tickangle=-45
        )
        fig_chuva.update_yaxes(title_text="Pluv. Horária (mm)", secondary_y=False);
        fig_chuva.update_yaxes(title_text=f"Acumulada ({selected_hours}h)", secondary_y=True)

        # (Lógica da Umidade - Mantida)
        df_umidade = df_ponto_plot.melt(id_vars=['timestamp_local'],
                                        value_vars=['umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc'],
                                        var_name='Sensor', value_name='Umidade Solo (%)')
        df_umidade['Sensor'] = df_umidade['Sensor'].replace({
            'umidade_1m_perc': '1m',
            'umidade_2m_perc': '2m',
            'umidade_3m_perc': '3m'
        })
        fig_umidade = px.line(df_umidade, x='timestamp_local', y='Umidade Solo (%)', color='Sensor',
                              title=f"Umidade Solo - Estação {config['nome']} ({n_horas_titulo}h)",
                              color_discrete_map=CORES_UMIDADE)
        fig_umidade.update_traces(line=dict(width=3))
        fig_umidade.update_layout(template=TEMPLATE_GRAFICO_MODERNO,
                                  margin=dict(l=40, r=20, t=40, b=80),
                                  legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor="center", x=0.5),
                                  xaxis_title="Data e Hora")
        fig_umidade.update_xaxes(
            dtick=3 * 60 * 60 * 1000,
            tickformat="%d/%m %Hh",
            tickangle=-45
        )

        # --- INÍCIO DA ATUALIZAÇÃO (Novo Gráfico de Inclinômetro) ---
        df_inclinometro = df_ponto_plot.melt(
            id_vars=['timestamp_local'],
            value_vars=['inclinometro_x', 'inclinometro_y'],
            var_name='Sensor',
            value_name='Inclinação (graus)'
        )

        # Renomeia para a legenda
        df_inclinometro['Sensor'] = df_inclinometro['Sensor'].replace({
            'inclinometro_x': 'Inclinômetro X',
            'inclinometro_y': 'Inclinômetro Y'
        })

        fig_inclinometro = px.line(
            df_inclinometro,
            x='timestamp_local',
            y='Inclinação (graus)',
            color='Sensor',
            title=f"Inclinômetro - Estação {config['nome']} ({n_horas_titulo}h)",
            color_discrete_map=CORES_INCLINOMETRO
        )
        fig_inclinometro.update_traces(line=dict(width=3))
        fig_inclinometro.update_layout(
            template=TEMPLATE_GRAFICO_MODERNO,
            margin=dict(l=40, r=20, t=40, b=80),
            legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor="center", x=0.5),
            xaxis_title="Data e Hora"
        )
        fig_inclinometro.update_xaxes(
            dtick=3 * 60 * 60 * 1000,
            tickformat="%d/%m %Hh",
            tickangle=-45
        )
        # --- FIM DA ATUALIZAÇÃO ---

        # Layout (Chuva e Umidade lado a lado)
        col_chuva = dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=fig_chuva)), className="shadow-sm"), width=12, lg=6,
                            className="mb-4")
        col_umidade = dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=fig_umidade)), className="shadow-sm"), width=12,
                              lg=6, className="mb-4")
        linha_ponto = dbc.Row([col_chuva, col_umidade], className="mb-4")
        layout_geral.append(linha_ponto)

        # --- INÍCIO DA ATUALIZAÇÃO (Adiciona nova linha para Inclinômetro) ---
        col_inclinometro = dbc.Col(dbc.Card(dbc.CardBody(dcc.Graph(figure=fig_inclinometro)), className="shadow-sm"),
                                   width=12, lg=12, className="mb-4")
        linha_inclinometro = dbc.Row([col_inclinometro], className="mb-4")
        layout_geral.append(linha_inclinometro)
        # --- FIM DA ATUALIZAÇÃO ---

    if not layout_geral: return dbc.Alert("Nenhum dado.", color="warning")
    return layout_geral