# app.py (CORRIGIDO E LIMPO - Sem diskcache)

import dash
import dash_bootstrap_components as dbc

# --- IMPORTAÇÃO CRÍTICA DO LEAFLET CSS ---
LEAFLET_CSS = [
    "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css",
    "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
]
# --- FIM DA IMPORTAÇÃO CRÍTICA ---

# Use um tema Bootstrap moderno.
THEME = dbc.themes.FLATLY

# Dizemos explicitamente ao Dash para carregar o nosso style.css da pasta /assets
MEU_CSS_LOCAL = [
    "/assets/style.css"
]

# --- Viewport Completa para Modo Desktop ---
META_TAGS = [
    {"name": "viewport",
     "content": "width=1200, initial-scale=0.25, minimum-scale=0.1, user-scalable=yes"}
]

app = dash.Dash(__name__,
                external_stylesheets=[THEME] + LEAFLET_CSS + MEU_CSS_LOCAL,
                meta_tags=META_TAGS,
                suppress_callback_exceptions=True,
                # Não há mais 'background_callback_manager'
                )

app.title = "Monitoramento Geoambiental"
server = app.server
TEMPLATE_GRAFICO_MODERNO = "plotly_white"