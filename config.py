# config.py (ATUALIZADO: Duas fontes de dados)

import os
import datetime
from dotenv import load_dotenv

# --- NOVA ABORDAGEM: Importa as bases do arquivo de calibração ---
try:
    from calibracao_base import bases_atuais as CONSTANTES_PADRAO
except ImportError:
    # Fallback para o caso do arquivo não existir
    CONSTANTES_PADRAO = {
        "UMIDADE_BASE_1M": 26.8, "UMIDADE_BASE_2M": 19.2, "UMIDADE_BASE_3M": 13.4,
        "UMIDADE_SATURACAO_1M": 47.0, "UMIDADE_SATURACAO_2M": 46.0, "UMIDADE_SATURACAO_3M": 49.0,
        "INCLINOMETRO_BASE_X": -17.7, "INCLINOMETRO_BASE_Y": 8.3
    }
# --- FIM DA NOVA ABORDAGEM ---


load_dotenv()

# --- INÍCIO DA ALTERAÇÃO (Duas Estações Físicas) ---
PLUGFIELD_CONFIG = {
    # Estação 10435: Usada para Umidade e Inclinômetros
    "Ponto-Sensores": {
        "STATION_ID": 10435,
        "API_KEY": "FLU4rvAmXy9kAIib6fG0q2ZBpFvpHc8rWZH16wqj",
        "USERNAME": "rafaela.skodowski@erione.com.br",
        "PASSWORD": "Rafaela15.15"
    },
    # Estação 00880 (ID 3182): Usada APENAS para Chuva
    "Ponto-Chuva": {
        "STATION_ID": 3182,
        "API_KEY": "FLU4rvAmXy9kAIib6fG0q2ZBpFvpHc8rWZH16wqj",
        "USERNAME": "rafaela.skodowski@erione.com.br",
        "PASSWORD": "Rafaela15.15"
    }
}

PLUGFIELD_SENSOR_MAP = {
    # Mapeamento da Estação 10435 (Sensores)
    "Ponto-Sensores": {
        # "chuva_mm": 35,  <-- REMOVIDO DESTA ESTAÇÃO
        "umidade_1m_perc": 30,
        "umidade_2m_perc": 85,
        "umidade_3m_perc": 87,
        "inclinometro_x": 41,
        "inclinometro_y": 42
    },
    # Mapeamento da Estação 3182 (Chuva)
    "Ponto-Chuva": {
        "chuva_mm": 35, # <-- ID 35 confirmado no seu log
    }
}
# --- FIM DA ALTERAÇÃO ---


# --- Configurações de DB (Mantidas) ---
DB_CONNECTION_STRING = os.getenv("DATABASE_URL", "sqlite:///temp_local_db.db")
DB_TABLE_NAME = "historico_monitoramento"
FREQUENCIA_API_SEGUNDOS = 60 * 15
MAX_HISTORICO_PONTOS = (72 * 60 * 60) // FREQUENCIA_API_SEGUNDOS

# --- Configurações dos Pontos de Análise ---
# !!! IMPORTANTE: Esta seção NÃO MUDA !!!
# O resto do app (dashboards, worker) continua vendo apenas o "Ponto-1".
# A "mágica" da união dos dados ocorre no data_source.py.
PONTOS_DE_ANALISE = {
    "Ponto-1": {
        "nome": "Estação Principal",
        "constantes": CONSTANTES_PADRAO.copy(),
        "lat_lon": [-23.156804878845303, -45.792735433490314]
    },
}

# --- Regras de Negócio (Alertas) (Mantido com últimos valores) ---
CHUVA_LIMITE_ATENCAO = 5.0
CHUVA_LIMITE_ALERTA = 10.0
CHUVA_LIMITE_ALERTA_MAXIMO = 15.0
DELTA_TRIGGER_UMIDADE = 2.0
INCLINOMETRO_DELTA_ATENCAO = 5.0
INCLINOMETRO_DELTA_ALERTA = 10.0
INCLINOMETRO_DELTA_ALERTA_MAXIMO = 15.0

RISCO_MAP = {
    "OBSERVAÇÃO": 0, "ATENÇÃO": 1, "ALERTA": 2, "ALERTA MÁXIMO": 3,
    "SEM DADOS": -1, "INDEFINIDO": -1, "ERRO": -1
}
STATUS_MAP_HIERARQUICO = {
    3: ("ALERTA MÁXIMO", "danger", "bg-danger"),
    2: ("ALERTA", "orange", "bg-orange"),
    1: ("ATENÇÃO", "warning", "bg-warning"),
    0: ("OBSERVAÇÃO", "success", "bg-success"),
    -1: ("SEM DADOS", "secondary", "bg-secondary")
}