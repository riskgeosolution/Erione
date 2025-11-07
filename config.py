# config.py (MODIFICADO: Adicionado Base do Inclinômetro e Limites DELTA)

import os
import datetime
from dotenv import load_dotenv

load_dotenv()

# --- API Plugfield (Mantido) ---
PLUGFIELD_CONFIG = {
    "Ponto-1": {
        "STATION_ID": 10435,
        "API_KEY": "FLU4rvAmXy9kAIib6fG0q2ZBpFvpHc8rWZH16wqj",
        "USERNAME": "rafaela.skodowski@erione.com.br",
        "PASSWORD": "Rafaela15.15"
    }
}
PLUGFIELD_SENSOR_MAP = {
    "Ponto-1": {
        "chuva_mm": 35,
        "umidade_1m_perc": 30,
        "umidade_2m_perc": 85,
        "umidade_3m_perc": 87,
        "inclinometro_x": 41,
        "inclinometro_y": 42
    }
}

# --- Configurações de DB (Mantidas) ---
DB_CONNECTION_STRING = os.getenv("DATABASE_URL", "sqlite:///temp_local_db.db")
DB_TABLE_NAME = "historico_monitoramento"
FREQUENCIA_API_SEGUNDOS = 60 * 15
MAX_HISTORICO_PONTOS = (72 * 60 * 60) // FREQUENCIA_API_SEGUNDOS

# --- Configurações dos Pontos de Análise (ATUALIZADO) ---
CONSTANTES_PADRAO = {
    "UMIDADE_BASE_1M": 30.0, "UMIDADE_BASE_2M": 36.0, "UMIDADE_BASE_3M": 39.0,
    "UMIDADE_SATURACAO_1M": 47.0,
    "UMIDADE_SATURACAO_2M": 46.0,
    "UMIDADE_SATURACAO_3M": 49.0,

    # --- INÍCIO DA ATUALIZAÇÃO (Valores Base do Inclinômetro) ---
    # (Valores de -17.7 e 8.3 baseados na sua imagem)
    "INCLINOMETRO_BASE_X": -17.7,
    "INCLINOMETRO_BASE_Y": 8.3
    # --- FIM DA ATUALIZAÇÃO ---
}

PONTOS_DE_ANALISE = {
    "Ponto-1": {
        "nome": "Estação Principal",
        "constantes": CONSTANTES_PADRAO.copy(),
        "lat_lon": [-23.15864795816037, -45.78894817006661]
    },
}

# --- Regras de Negócio (Alertas) (ATUALIZADO) ---
CHUVA_LIMITE_VERDE = 60.0
CHUVA_LIMITE_AMARELO = 79.0
CHUVA_LIMITE_LARANJA = 100.0

DELTA_TRIGGER_UMIDADE = 3.0

# --- INÍCIO DA ATUALIZAÇÃO (Limites de VARIAÇÃO/DELTA do Inclinômetro) ---
# (Assumindo que uma variação de 5 graus é Alerta e 10 é Paralisação)
INCLINOMETRO_DELTA_AMARELO = 5.0  # Variação > 5 graus = ALERTA
INCLINOMETRO_DELTA_LARANJA = 10.0  # Variação >= 10 graus = PARALIZAÇÃO
# --- FIM DA ATUALIZAÇÃO ---

RISCO_MAP = {
    "LIVRE": 0,
    "ATENÇÃO": 1,
    "ALERTA": 2,
    "PARALIZAÇÃO": 3,
    "SEM DADOS": -1,
    "INDEFINIDO": -1,
    "ERRO": -1
}
STATUS_MAP_HIERARQUICO = {
    3: ("PARALIZAÇÃO", "danger", "bg-danger"),
    2: ("ALERTA", "orange", "bg-orange"),
    1: ("ATENÇÃO", "warning", "bg-warning"),
    0: ("LIVRE", "success", "bg-success"),
    -1: ("SEM DADOS", "secondary", "bg-secondary")
}