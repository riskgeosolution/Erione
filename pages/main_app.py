# pages/main_app.py (CORREÇÃO FINAL: Sincroniza o estado inicial do Switch e do Label)

import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import importlib

from app import app
import calibracao_base # Importa para ler o estado inicial


def get_navbar():
    """ Retorna a barra de navegação azul. """
    logo_riskgeo_path = app.get_asset_url('LogoMarca RiskGeo Solutions.png')
    logo_tamoios_path = app.get_asset_url('tamoios.png')
    cor_fundo_navbar = '#003366'
    nova_altura_logo = "50px"

    # --- LÊ O ESTADO INICIAL DO ARQUIVO DE CONFIGURAÇÃO ---
    importlib.reload(calibracao_base)
    initial_switch_value = calibracao_base.bases_atuais.get("API_AUTO_ATIVADA", False)
    
    # --- DEFINE O ESTADO INICIAL DO LABEL COM BASE NO VALOR DO SWITCH ---
    initial_label_text = "ON" if initial_switch_value else "OFF"
    initial_label_class = "ms-2 bg-light-green" if initial_switch_value else "ms-2"
    # --- FIM DA CORREÇÃO ---

    navbar = dbc.Navbar(
        dbc.Container(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            dbc.Row(
                                [
                                    dbc.Col(html.A(html.Img(src=logo_tamoios_path, height=nova_altura_logo), href="/"), width="auto"),
                                    dbc.Col(html.Img(src=logo_riskgeo_path, height=nova_altura_logo, className="ms-3"), width="auto"),
                                ],
                                align="center",
                                className="g-0",
                            ),
                            width="auto",
                        ),
                        dbc.Col(
                            html.H4("SISTEMA DE MONITORAMENTO GEOAMBIENTAL", className="mb-0 text-center",
                                    style={'fontWeight': 'bold', 'color': 'white', 'font-size': '1.3rem'}),
                            width="auto",
                        ),
                        dbc.Col(
                            dbc.Nav(
                                [
                                    dbc.NavItem(dbc.NavLink("Mapa Geral", href="/", active="exact", className="text-light", style={'font-size': '1.0rem', 'font-weight': '500'})),
                                    dbc.NavItem(dbc.NavLink("Dashboard Geral", href="/dashboard-geral", active="exact", className="text-light ms-3", style={'font-size': '1.0rem', 'font-weight': '500'})),
                                    dbc.NavItem(
                                        dbc.InputGroup(
                                            [
                                                dbc.Switch(
                                                    id='switch-api-auto',
                                                    value=initial_switch_value, # <-- Usa o valor lido do arquivo
                                                    className="ms-3",
                                                    persistence=True,
                                                    persistence_type='session'
                                                ),
                                                # --- CORREÇÃO: Usa as variáveis de estado inicial ---
                                                dbc.InputGroupText(
                                                    id='switch-api-label',
                                                    children=initial_label_text,
                                                    style={'font-size': '0.8rem', 'font-weight': 'bold'},
                                                    className=initial_label_class
                                                ),
                                                # --- FIM DA CORREÇÃO ---
                                            ],
                                            className="ms-4"
                                        ),
                                        className="d-flex align-items-center"
                                    ),
                                    dbc.NavItem(
                                        dbc.Button("Sair", id='logout-button', color="danger", className="ms-4", n_clicks=0),
                                        className="d-flex align-items-center"
                                    ),
                                    html.Button(id='navbar-load-trigger', n_clicks=0, style={'display': 'none'})
                                ],
                                navbar=True,
                                className="flex-nowrap",
                            ),
                            width="auto",
                        ),
                    ],
                    align="center",
                    className="w-100 flex-nowrap",
                    justify="between",
                ),
            ],
            fluid=True,
            id='navbar-main-container'
        ),
        style={'backgroundColor': cor_fundo_navbar},
        dark=True,
        className="mb-4"
    )
    return navbar


def get_layout():
    """ 
    Retorna o layout principal do app (depois do login).
    """
    return html.Div(id='page-content')
