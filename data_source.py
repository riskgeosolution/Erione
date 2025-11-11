# data_source.py (COMPLETO: Alterado para PostgreSQL e tabela de estado)

import pandas as pd
import json
import os
import datetime
import httpx
import traceback
import warnings
import time
from sqlalchemy import create_engine, inspect, text, exc
from io import StringIO

warnings.simplefilter(action='ignore', category=FutureWarning)

from config import (
    PONTOS_DE_ANALISE,
    FREQUENCIA_API_SEGUNDOS,
    MAX_HISTORICO_PONTOS,
    PLUGFIELD_CONFIG,
    PLUGFIELD_SENSOR_MAP,
    DB_CONNECTION_STRING,
    DB_TABLE_NAME
)

# --- Configurações de Disco (Caminhos) ---
# Estes arquivos (status e log) serão TEMPORÁRIOS no Render (e tudo bem)
DATA_DIR = "."
HISTORICO_FILE_CSV = os.path.join(DATA_DIR, "historico_temp.csv")  # Não será mais usado para dados
STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
LOG_FILE = os.path.join(DATA_DIR, "eventos.log")
LAST_UPDATE_FILE = os.path.join(DATA_DIR, "last_api_update.json")

COLUNAS_HISTORICO = [
    'timestamp', 'id_ponto', 'chuva_mm', 'precipitacao_acumulada_mm',
    'umidade_1m_perc', 'umidade_2m_perc', 'umidade_3m_perc',
    'base_1m', 'base_2m', 'base_3m',
    'inclinometro_x', 'inclinometro_y'
]

PLUGFIELD_TOKEN_CACHE = {}

# --- NOVOS VALORES PADRÃO (Substituindo calibracao_base.py) ---
VALORES_PADRAO_ESTADO = {
    "API_AUTO_ATIVADA": "False",
    "UMIDADE_BASE_1M": "26.8",
    "UMIDADE_BASE_2M": "16.2",
    "UMIDADE_BASE_3M": "13.4",
    "INCLINOMETRO_BASE_X": "-17.7",
    "INCLINOMETRO_BASE_Y": "8.3",
}


# -----------------------------------------------

# ==========================================================
# --- FUNÇÕES AUXILIARES DA API (Sem alterações) ---
# ==========================================================
def _get_plugfield_token(id_ponto):
    global PLUGFIELD_TOKEN_CACHE
    if id_ponto in PLUGFIELD_TOKEN_CACHE:
        return PLUGFIELD_TOKEN_CACHE[id_ponto], None
    print(f"[API Plugfield] Token não encontrado no cache. Efetuando login para {id_ponto}...")
    try:
        config = PLUGFIELD_CONFIG[id_ponto]
        BASE_URL = "https://prod-api.plugfield.com.br"
        login_url = f"{BASE_URL}/login"
        headers = {'x-api-key': config['API_KEY'], 'Content-Type': 'application/json'}
        login_data = {"username": config['USERNAME'], "password": config['PASSWORD']}
        with httpx.Client(timeout=20.0) as client:
            response = client.post(login_url, headers=headers, data=json.dumps(login_data))
        if response.status_code == 200:
            access_token = response.json().get("access_token")
            if access_token:
                print(f"[API Plugfield] Login para {id_ponto} bem-sucedido.")
                PLUGFIELD_TOKEN_CACHE[id_ponto] = access_token
                return access_token, None
            else:
                return None, f"ERRO API Plugfield: Login OK (200) mas 'access_token' não foi retornado."
        elif response.status_code == 403:
            return None, f"ERRO API Plugfield: Falha no login (403). Resposta: {response.text}"
        else:
            return None, f"ERRO API Plugfield: Falha no login ({response.status_code}). Resposta: {response.text}"
    except Exception as e:
        return None, f"ERRO CRÍTICO (Plugfield Login): {e}"


def _fetch_plugfield_sensor_data(token, api_key, station_id, sensor_id, start_ms, end_ms, sensor_name_log=""):
    sensor_url = "https://prod-api.plugfield.com.br/data/sensor"
    headers = {'x-api-key': api_key, 'Authorization': token}
    params = {
        'device': station_id, 'sensor': sensor_id,
        'time': start_ms, 'timeMax': end_ms
    }
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.get(sensor_url, headers=headers, params=params)
        if response.status_code == 200:
            dados = response.json().get('data', [])
            print(f"      - Sensor {sensor_name_log} (ID {sensor_id}): {len(dados)} registros encontrados.")
            return dados, None
        elif response.status_code == 401:
            print(f"      - Token inválido (401) para sensor {sensor_id}. Limpando cache de token.")
            global PLUGFIELD_TOKEN_CACHE
            for key, value in list(PLUGFIELD_TOKEN_CACHE.items()):
                if value == token:
                    del PLUGFIELD_TOKEN_CACHE[key]
            return [], f"Token de acesso expirou (401). Será tentado novo login no próximo ciclo."
        else:
            return [], f"Erro ao buscar sensor {sensor_id} ({response.status_code}): {response.text}"
    except Exception as e:
        return [], f"Erro de conexão ao buscar sensor {sensor_id}: {e}"


def fetch_and_process_plugfield_data(df_historico_existente):
    logs_api = []
    lista_dataframes_finais = []

    # --- ALTERAÇÃO: Busca 72h se o DB estiver vazio ---
    DATA_INICIO_PADRAO = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=72)
    # --- FIM DA ALTERAÇÃO ---

    for id_ponto, config in PONTOS_DE_ANALISE.items():
        print(f"[API Plugfield] Iniciando coleta para: {id_ponto}")
        try:
            ponto_config = PLUGFIELD_CONFIG[id_ponto]
            station_id = ponto_config["STATION_ID"]
            api_key = ponto_config["API_KEY"]
            sensor_map = PLUGFIELD_SENSOR_MAP[id_ponto]
        except KeyError:
            logs_api.append({"id_ponto": id_ponto, "mensagem": f"ERRO: {id_ponto} não encontrado em PLUGFIELD_CONFIG."})
            continue
        token, erro_token = _get_plugfield_token(id_ponto)
        if erro_token:
            logs_api.append({"id_ponto": id_ponto, "mensagem": erro_token})
            if "403" in erro_token:
                return pd.DataFrame(), logs_api
            continue

        df_ponto_existente = df_historico_existente[df_historico_existente['id_ponto'] == id_ponto]
        agora_utc = datetime.datetime.now(datetime.timezone.utc)

        # --- ALTERAÇÃO: Lógica de data de início ---
        if df_ponto_existente.empty:
            start_dt_utc = DATA_INICIO_PADRAO
            print(f"[API Plugfield] Histórico vazio. Buscando dados desde {start_dt_utc.isoformat()} (72h atrás).")
        else:
            start_dt_utc = df_ponto_existente['timestamp'].max()
            if start_dt_utc < DATA_INICIO_PADRAO:
                start_dt_utc = DATA_INICIO_PADRAO
            print(f"[API Plugfield] Buscando dados desde {start_dt_utc.isoformat()}")
        # --- FIM DA ALTERAÇÃO ---

        start_dt_utc += datetime.timedelta(seconds=1)
        end_time_ms = int(agora_utc.timestamp() * 1000)
        start_time_ms = int(start_dt_utc.timestamp() * 1000)
        if start_time_ms >= end_time_ms:
            print(f"[API Plugfield] {id_ponto} já está atualizado. Nenhum dado novo para buscar.")
            continue
        dados_brutos_sensores = {}
        for nome_app, sensor_id in sensor_map.items():
            dados, erro = _fetch_plugfield_sensor_data(token, api_key, station_id, sensor_id, start_time_ms,
                                                       end_time_ms, sensor_name_log=nome_app)
            if erro:
                logs_api.append({"id_ponto": id_ponto, "mensagem": erro})
            if dados:
                dados_brutos_sensores[nome_app] = dados
        if not dados_brutos_sensores:
            print(f"[API Plugfield] Nenhum dado retornado pela API para {id_ponto} no período.")
            continue
        lista_dfs_processados = []
        for nome_app, dados_brutos in dados_brutos_sensores.items():
            df_sensor = pd.DataFrame(dados_brutos)
            if df_sensor.empty:
                continue
            df_sensor = df_sensor[['time', 'valueFormatted']]
            df_sensor['timestamp'] = pd.to_datetime(df_sensor['time'], unit='ms', utc=True)
            df_sensor = df_sensor.set_index('timestamp')
            df_sensor = df_sensor.rename(columns={'valueFormatted': nome_app})
            df_sensor = df_sensor.drop(columns=['time'])
            df_sensor = df_sensor[~df_sensor.index.duplicated(keep='last')]
            lista_dfs_processados.append(df_sensor)
        if not lista_dfs_processados:
            print(f"[API Plugfield] Processamento não gerou DataFrames para {id_ponto}.")
            continue
        df_ponto_final = pd.concat(lista_dfs_processados, axis=1, join='outer')
        df_ponto_final = df_ponto_final.interpolate(method='time', limit_area='inside', limit=4)
        df_ponto_final = df_ponto_final.sort_index()
        df_ponto_final['id_ponto'] = id_ponto
        lista_dataframes_finais.append(df_ponto_final.reset_index())
    if not lista_dataframes_finais:
        return pd.DataFrame(), logs_api
    df_resultado_final = pd.concat(lista_dataframes_finais, ignore_index=True)
    df_resultado_final = df_resultado_final.rename(columns={'index': 'timestamp'})
    logs_api.append({"id_ponto": "GERAL",
                     "mensagem": f"API: Sucesso. {len(df_resultado_final)} novos registros totais processados."})
    return df_resultado_final, logs_api


# ==========================================================
# --- FUNÇÕES DE LOG E CONFIGURAÇÃO DE CAMINHO ---
# ==========================================================
def adicionar_log(id_ponto, mensagem):
    try:
        log_entry = f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} | {id_ponto} | {mensagem}\n"
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_entry)
    except Exception as e:
        print(f"ERRO CRÍTICO ao escrever no log: {e}")


def setup_disk_paths():
    print("--- data_source.py (Modo PostgreSQL) ---")
    global DATA_DIR, STATUS_FILE, LOG_FILE, HISTORICO_FILE_CSV, DB_CONNECTION_STRING, LAST_UPDATE_FILE

    # --- ALTERAÇÃO: Remove a lógica do RENDER e do DISCO ---
    # O DB_CONNECTION_STRING virá 100% da variável de ambiente
    # Os arquivos de log/status serão salvos localmente (e serão temporários no Render)
    DATA_DIR = "."
    # --- FIM DA ALTERAÇÃO ---

    STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
    LOG_FILE = os.path.join(DATA_DIR, "eventos.log")
    HISTORICO_FILE_CSV = os.path.join(DATA_DIR, "historico_temp.csv")  # Não mais usado
    LAST_UPDATE_FILE = os.path.join(DATA_DIR, "last_api_update.json")
    print(f"Caminho do Disco de Dados (Temporário): {DATA_DIR}")
    print(f"Banco de Dados (Escrita): {DB_CONNECTION_STRING}")


# ==========================================================
# --- FUNÇÕES DE BANCO de DADOS (ALTERADAS) ---
# ==========================================================
def get_engine():
    # Esta função agora é a única fonte do engine.
    # DB_CONNECTION_STRING é pego do os.getenv("DATABASE_URL") em config.py
    return create_engine(DB_CONNECTION_STRING)


# --- NOVA FUNÇÃO: Gerencia a tabela de estado (app_state) ---
def _init_state_table(engine):
    """Garante que a tabela app_state exista e tenha valores padrão."""
    DB_STATE_TABLE = "app_state"
    try:
        with engine.connect() as connection:
            # Tenta criar a tabela
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {DB_STATE_TABLE} (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            """))
            # Popula com valores padrão (apenas se não existirem)
            for key, value in VALORES_PADRAO_ESTADO.items():
                connection.execute(text(f"""
                    INSERT INTO {DB_STATE_TABLE} (key, value)
                    VALUES (:key, :value)
                    ON CONFLICT(key) DO NOTHING;
                """), {"key": key, "value": value})
            connection.commit()
        print(f"[DB] Tabela '{DB_STATE_TABLE}' inicializada.")
    except Exception as e:
        print(f"ERRO CRÍTICO ao inicializar a tabela de estado: {e}")


# --- NOVAS FUNÇÕES: get/set para o estado (substitui calibracao_base.py) ---
def get_app_state(key):
    """Busca um valor da tabela de estado (ex: 'API_AUTO_ATIVADA')"""
    DB_STATE_TABLE = "app_state"
    engine = get_engine()
    try:
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT value FROM {DB_STATE_TABLE} WHERE key = :key"), {"key": key})
            value = result.scalar()  # Retorna o primeiro valor da primeira linha
            if value is None:
                # Se a chave não existe, retorna o padrão e tenta salvar
                default_value = VALORES_PADRAO_ESTADO.get(key)
                set_app_state(key, default_value)  # Tenta criar
                return default_value
            return value
    except exc.OperationalError as e:
        # Erro comum se o DB não existe (ex: teste local sem postgres)
        print(f"AVISO: Não foi possível ler o estado do DB ({e}). Retornando padrão.")
        return VALORES_PADRAO_ESTADO.get(key)
    except Exception as e:
        print(f"ERRO ao ler estado '{key}': {e}. Retornando padrão.")
        return VALORES_PADRAO_ESTADO.get(key)


def set_app_state(key, value):
    """Salva um valor na tabela de estado (ex: 'API_AUTO_ATIVADA', 'True')"""
    DB_STATE_TABLE = "app_state"
    engine = get_engine()
    try:
        with engine.connect() as connection:
            # ON CONFLICT(key) DO UPDATE é o "upsert" (cria se não existe, atualiza se existe)
            connection.execute(text(f"""
                INSERT INTO {DB_STATE_TABLE} (key, value)
                VALUES (:key, :value)
                ON CONFLICT(key) DO UPDATE SET value = :value;
            """), {"key": key, "value": str(value)})
            connection.commit()
        return True
    except Exception as e:
        print(f"ERRO ao salvar estado '{key}': {e}")
        return False


# --- FIM DAS NOVAS FUNÇÕES DE ESTADO ---


def save_to_db(df_novos_dados):
    if df_novos_dados.empty:
        return
    try:
        engine = get_engine()
        inspector = inspect(engine)

        # --- ALTERAÇÃO: Inicializa a tabela de estado também ---
        if not inspector.has_table("app_state"):
            _init_state_table(engine)
        # --- FIM DA ALTERAÇÃO ---

        if not inspector.has_table(DB_TABLE_NAME):
            print(f"[DB] Tabela '{DB_TABLE_NAME}' não encontrada. Criando...")
            # (A definição da tabela de histórico permanece a mesma)
            colunas_db = {
                'timestamp': 'TIMESTAMP', 'id_ponto': 'TEXT', 'chuva_mm': 'REAL',
                'precipitacao_acumulada_mm': 'REAL', 'umidade_1m_perc': 'REAL',
                'umidade_2m_perc': 'REAL', 'umidade_3m_perc': 'REAL',
                'base_1m': 'REAL', 'base_2m': 'REAL', 'base_3m': 'REAL',
                'inclinometro_x': 'REAL', 'inclinometro_y': 'REAL'
            }
            # Sintaxe do PostgreSQL é um pouco diferente para UNIQUE
            create_query = f"CREATE TABLE {DB_TABLE_NAME} (\n"
            create_query += ",\n".join([f'"{col}" {tipo}' for col, tipo in colunas_db.items()])
            create_query += f",\nUNIQUE(id_ponto, \"timestamp\")\n);"

            with engine.connect() as connection:
                connection.execute(text(create_query))
                connection.commit()
            print(f"[DB] Tabela '{DB_TABLE_NAME}' criada com sucesso.")

        cols_tabela_db = [col['name'] for col in inspector.get_columns(DB_TABLE_NAME)]
        cols_para_salvar = [col for col in df_novos_dados.columns if col in cols_tabela_db]
        df_para_salvar = df_novos_dados[cols_para_salvar]

        # Usar 'to_sql' com PostgreSQL pode ser lento.
        # Uma inserção direta é mais robusta se 'to_sql' falhar.
        df_para_salvar.to_sql(DB_TABLE_NAME, engine, if_exists='append', index=False, method=None)

        print(f"[DB] {len(df_para_salvar)} novos pontos salvos no DB.")
    except exc.IntegrityError:  # Erro de 'UNIQUE constraint failed'
        print(f"[DB] Aviso: Dados duplicados para este timestamp. Ignorando.")
    except Exception as e:
        adicionar_log("GERAL", f"ERRO CRÍTICO ao salvar no DB: {e}")
        print(f"ERRO CRÍTICO ao salvar no DB: {e}")
        traceback.print_exc()


# (Função migrar_csv_to_sqlite_initial removida)

def read_data_from_db(id_ponto, start_dt, end_dt):
    print(f"[DB] Consultando dados para {id_ponto} de {start_dt} a {end_dt}")
    engine = get_engine()
    inspector = inspect(engine)
    if not inspector.has_table(DB_TABLE_NAME):
        print(f"[DB] Tabela '{DB_TABLE_NAME}' não existe. Retornando vazio.")
        # --- ALTERAÇÃO: Tenta criar as tabelas se não existirem ---
        save_to_db(pd.DataFrame())  # Chama a função com DF vazio para forçar a criação da tabela
        # --- FIM DA ALTERAÇÃO ---
        return pd.DataFrame(columns=COLUNAS_HISTORICO)

    start_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    query = f"""
        SELECT * FROM {DB_TABLE_NAME}
        WHERE id_ponto = :ponto
        AND timestamp >= :start
        AND timestamp < :end
        ORDER BY timestamp ASC
    """
    try:
        df = pd.read_sql_query(
            query,
            engine,
            params={"ponto": id_ponto, "start": start_str, "end": end_str},
            parse_dates=["timestamp"]
        )
        if not df.empty and 'timestamp' in df.columns:
            if df['timestamp'].dt.tz is None:
                df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
        for col in COLUNAS_HISTORICO:
            if col not in df.columns:
                df[col] = pd.NA
        return df[COLUNAS_HISTORICO]
    except Exception as e:
        print(f"ERRO CRÍTICO ao ler do DB: {e}")
        adicionar_log("GERAL", f"ERRO CRÍTICO ao ler do DB: {e}")
        return pd.DataFrame(columns=COLUNAS_HISTORICO)


# (Funções de CSV e Sincronia removidas ou não são mais necessárias)


# ==========================================================
# --- FUNÇÃO DE LEITURA PRINCIPAL PARA O DASHBOARD (ALTERADA) ---
# ==========================================================
def get_all_data_from_disk(worker_mode=False):
    """
    Lê os dados do banco de dados (PostgreSQL).
    A lógica de CSV (worker_mode=True) foi removida.
    """

    print("[Dashboard] Lendo dados para o dashboard A PARTIR DO DB (Últimos 14 dias)...")
    agora_utc = datetime.datetime.now(datetime.timezone.utc)
    end_dt = agora_utc
    start_dt = agora_utc - datetime.timedelta(days=14)
    lista_dfs = []
    for id_ponto in PONTOS_DE_ANALISE.keys():
        # A função read_data_from_db agora lê do Postgres
        df_ponto = read_data_from_db(id_ponto, start_dt, end_dt)
        if not df_ponto.empty:
            lista_dfs.append(df_ponto)
    if lista_dfs:
        historico_df = pd.concat(lista_dfs, ignore_index=True)
        print(f"[Dashboard] {len(historico_df)} registros lidos do DB para exibição.")
    else:
        print("[Dashboard] Nenhum dado encontrado no DB para o período.")
        historico_df = pd.DataFrame(columns=COLUNAS_HISTORICO)

    # (A lógica de ler status.json e eventos.log permanece, são arquivos temporários)
    default_status = {
        "geral": "INDEFINIDO", "chuva": "INDEFINIDO", "umidade": "INDEFINIDO",
        "inclinometro_x": "INDEFINIDO", "inclinometro_y": "INDEFINIDO"
    }
    try:
        with open(STATUS_FILE, 'r', encoding='utf-8') as f:
            status_atual = json.load(f)
        if not status_atual or not isinstance(list(status_atual.values())[0], dict):
            status_atual = {p: default_status for p in PONTOS_DE_ANALISE.keys()}
    except (FileNotFoundError, json.JSONDecodeError, IndexError):
        status_atual = {p: default_status for p in PONTOS_DE_ANALISE.keys()}
    except Exception as e:
        print(f"ERRO ao ler {STATUS_FILE}: {e}.")
        status_atual = {p: default_status for p in PONTOS_DE_ANALISE.keys()}

    try:
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            logs = f.read()
    except FileNotFoundError:
        logs = "Nenhum evento registrado ainda."
    except Exception:
        logs = "Erro ao ler arquivo de log."
    return historico_df, status_atual, logs


# ==========================================================
# --- FUNÇÃO PRINCIPAL DE REQUISIÇÃO E SALVAMENTO (ALTERADA) ---
# ==========================================================
def executar_passo_api_e_salvar(historico_df_existente):
    """
    historico_df_existente é lido do DB (não mais do CSV).
    Salva os novos dados no DB (não mais no CSV).
    """
    try:
        dados_api_df, logs_api = fetch_and_process_plugfield_data(historico_df_existente)
        status_novos = None
        for log in logs_api:
            mensagem_log_completa = f"| {log['id_ponto']} | {log['mensagem']}"
            print(mensagem_log_completa)
            adicionar_log(log['id_ponto'], log['mensagem'])
    except Exception as e:
        adicionar_log("GERAL", f"ERRO CRÍTICO (fetch_data): {e}")
        traceback.print_exc()
        return pd.DataFrame(), None
    if dados_api_df.empty:
        print("[Worker] API Plugfield não retornou novos dados neste ciclo.")
        return pd.DataFrame(), status_novos
    try:
        if 'timestamp' in dados_api_df.columns:
            dados_api_df['timestamp'] = pd.to_datetime(dados_api_df['timestamp'], utc=True)
        dados_api_df['chuva_mm'] = pd.to_numeric(dados_api_df['chuva_mm'], errors='coerce').fillna(0.0)
        for col in COLUNAS_HISTORICO:
            if col not in dados_api_df.columns:
                dados_api_df[col] = pd.NA

        # --- ALTERAÇÃO: Salva no DB (Postgres) ---
        save_to_db(dados_api_df)

        # --- REMOVIDO: Não salva mais no CSV ---
        # historico_atualizado_df_csv = pd.concat([historico_df_csv, dados_api_df], ignore_index=True)
        # save_historico_to_csv(historico_atualizado_df_csv)

        # --- REMOVIDO: Não salva mais o last_api_update.json ---
        # (A lógica agora é "buscar desde o último timestamp no DB")

        return dados_api_df, status_novos
    except Exception as e:
        adicionar_log("GERAL", f"ERRO CRÍTICO (processar/salvar): {e}")
        traceback.print_exc()
        return pd.DataFrame(), status_novos