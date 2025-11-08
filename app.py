# app.py (CORRIGIDO: Usando background_callback_manager)

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

# --- INÍCIO DA SOLUÇÃO DO ERRO DE BACKGROUND ---
from dash import DiskcacheManager as DiskcacheLongCallbackManager
import diskcache
import os

# Define a pasta onde o cache será armazenado
CACHE_DIR = os.path.join(os.getcwd(), "cache_callbacks")
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

cache = diskcache.Cache(CACHE_DIR)
# O DiskcacheLongCallbackManager agora é importado como DiskcacheManager
long_callback_manager = DiskcacheLongCallbackManager(cache)
# --- FIM DA SOLUÇÃO ---


app = dash.Dash(__name__,
                external_stylesheets=[THEME] + LEAFLET_CSS + MEU_CSS_LOCAL,
                meta_tags=META_TAGS,
                suppress_callback_exceptions=True,

                # --- APLICA O GERENCIADOR DE CALLBACKS LONGOS ---
                # ** Linha Corrigida: Renomeado para background_callback_manager **
                background_callback_manager=long_callback_manager
                # -----------------------------------------------
                )

app.title = "Monitoramento Geoambiental"
server = app.server
TEMPLATE_GRAFICO_MODERNO = "plotly_white"