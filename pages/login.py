# pages/login.py (CORRIGIDO: Card de login mais estreito)

import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from app import app
import datetime


def get_layout():
    """ Retorna o layout da página de login. """

    logo_riskgeo_path = app.get_asset_url('LogoMarca RiskGeo Solutions.png')
    logo_tamoios_path = app.get_asset_url('tamoios.png')
    fundo_path = app.get_asset_url('tamoios_fundo.png')
    nova_altura_logo = "90px"

    style_fundo = {
        'backgroundImage': f"url('{fundo_path}')",
        'backgroundSize': 'cover',
        'backgroundPosition': 'center',
        'backgroundRepeat': 'no-repeat'
    }

    layout = dbc.Container([
        dbc.Row(
            dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        dbc.Row(
                            [
                                dbc.Col(html.Img(src=logo_tamoios_path, height=nova_altura_logo), width="auto"),
                                dbc.Col(html.Img(src=logo_riskgeo_path, height=nova_altura_logo), width="auto"),
                            ],
                            align="center",
                            justify="around",
                            className="mb-4 pt-3"
                        ),
                        html.H4("Sistema de Monitoramento Geoambiental", className="card-title text-center mb-4 login-title"),
                        html.Div(id='login-error-output', className="text-danger mb-3 text-center"),
                        dbc.Row(
                            dbc.Col(
                                dbc.Input(
                                    id='input-password',
                                    type='password',
                                    placeholder='Digite sua senha',
                                    n_submit=0
                                ),
                                width=10,
                                lg=6
                            ),
                            justify="center",
                            className="mb-4"
                        ),
                        dbc.Row(
                            dbc.Col(
                                dbc.Button(
                                    "Acessar",
                                    id='btn-login',
                                    color='primary',
                                    style={'font-size': '1.1rem', 'font-weight': 'bold', 'padding': '0.5rem 2rem'},
                                    n_clicks=0
                                ),
                                width="auto"
                            ),
                            justify="center",
                            className="mb-3"
                        ),
                        html.P(
                            f"© {datetime.datetime.now().year} RiskGeo Solutions Engenharia e Consultoria Ltda. Todos os direitos reservados.",
                            className="text-center small mt-3 mb-0 copyright-text-yellow"
                        )
                    ])
                ], className='login-card'),
                # --- ALTERAÇÃO: Diminuída a largura do card ---
                width=12, md=8, lg=6
                # --- FIM DA ALTERAÇÃO ---
            ),
            justify="center",
            align="center",
            className="vh-100"
        )
    ], fluid=True, style=style_fundo)

    return layout