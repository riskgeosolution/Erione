# data_source.py (COMPLETO: Ordem de Funções Corrigida para NameError)

import pandas as pd
import json
import os
import datetime
import httpx
import traceback
import warnings
import time
from sqlalchemy import create_engine, inspect, text
from io import StringIO

warnings.simplefilter(action='ignore', category=FutureWarning)

from config import (
    PONTOS_DE_ANALISE, CONSTANTES_PADRAO,
    FREQUENCIA_API_SEGUNDOS,
    MAX_HISTORICO_PONTOS,
    PLUGFIELD_CONFIG,
    PLUGFIELD_SENSOR_MAP,
    DB_CONNECTION_STRING,
    DB_TABLE_NAME
)

# --- Configurações de Disco (Caminhos) ---
DATA_DIR = "."
HISTORICO_FILE_CSV = os.path.join(DATA_DIR, "historico_temp.csv")
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


# ==========================================================
# --- FUNÇÕES AUXILIARES DA API (MOVIDAS PARA O TOPO) ---
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
    DATA_INICIO_FIXA = datetime.datetime(2025, 11, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
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
        if df_ponto_existente.empty:
            start_dt_utc = DATA_INICIO_FIXA
            print(f"[API Plugfield] Histórico vazio. Buscando dados desde {start_dt_utc.isoformat()}.")
        else:
            start_dt_utc = df_ponto_existente['timestamp'].max()
            if start_dt_utc < DATA_INICIO_FIXA:
                start_dt_utc = DATA_INICIO_FIXA
            print(f"[API Plugfield] Buscando dados desde {start_dt_utc.isoformat()}")
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
    print("--- data_source.py (Plugfield Mode) ---")
    global DATA_DIR, STATUS_FILE, LOG_FILE, HISTORICO_FILE_CSV, DB_CONNECTION_STRING, LAST_UPDATE_FILE
    if os.environ.get('RENDER'):
        DATA_DIR = "/var/data"
        DB_CONNECTION_STRING = f'sqlite:///{DATA_DIR}/temp_local_db.db'
    else:
        DATA_DIR = "."
    STATUS_FILE = os.path.join(DATA_DIR, "status_atual.json")
    LOG_FILE = os.path.join(DATA_DIR, "eventos.log")
    HISTORICO_FILE_CSV = os.path.join(DATA_DIR, "historico_temp.csv")
    LAST_UPDATE_FILE = os.path.join(DATA_DIR, "last_api_update.json")
    print(f"Caminho do Disco de Dados: {DATA_DIR}")
    print(f"Banco de Dados (Escrita): {DB_CONNECTION_STRING}")


# ==========================================================
# --- FUNÇÕES DE BANCO de DADOS (Mantidas) ---
# ==========================================================
def get_engine():
    return create_engine(DB_CONNECTION_STRING)


def save_to_sqlite(df_novos_dados):
    if df_novos_dados.empty:
        return
    try:
        engine = get_engine()
        inspector = inspect(engine)
        if not inspector.has_table(DB_TABLE_NAME):
            print(f"[SQLite] Tabela '{DB_TABLE_NAME}' não encontrada. Criando...")
            colunas_db = {
                'timestamp': 'TIMESTAMP', 'id_ponto': 'TEXT', 'chuva_mm': 'REAL',
                'precipitacao_acumulada_mm': 'REAL', 'umidade_1m_perc': 'REAL',
                'umidade_2m_perc': 'REAL', 'umidade_3m_perc': 'REAL',
                'base_1m': 'REAL', 'base_2m': 'REAL', 'base_3m': 'REAL',
                'inclinometro_x': 'REAL', 'inclinometro_y': 'REAL'
            }
            create_query = f"CREATE TABLE {DB_TABLE_NAME} (\n"
            create_query += ",\n".join([f'"{col}" {tipo}' for col, tipo in colunas_db.items()])
            create_query += ",\nUNIQUE(id_ponto, \"timestamp\")\n);"
            with engine.connect() as connection:
                connection.execute(text(create_query))
            print(f"[SQLite] Tabela '{DB_TABLE_NAME}' criada com sucesso.")
        cols_tabela_db = [col['name'] for col in inspector.get_columns(DB_TABLE_NAME)]
        cols_para_salvar = [col for col in df_novos_dados.columns if col in cols_tabela_db]
        df_para_salvar = df_novos_dados[cols_para_salvar]
        df_para_salvar.to_sql(DB_TABLE_NAME, engine, if_exists='append', index=False)
        print(f"[SQLite] {len(df_para_salvar)} novos pontos salvos no DB.")
    except Exception as e:
        if "UNIQUE constraint failed" in str(e):
            print(f"[SQLite] Aviso: Dados duplicados para este timestamp. Ignorando.")
        else:
            adicionar_log("GERAL", f"ERRO CRÍTICO ao salvar no SQLite: {e}")
            print(f"ERRO CRÍTICO ao salvar no SQLite: {e}")


def migrate_csv_to_sqlite_initial():
    # (Mantida)
    engine = get_engine()
    inspector = inspect(engine)
    try:
        if inspector.has_table(DB_TABLE_NAME):
            with engine.connect() as connection:
                query = text(f"SELECT COUNT(1) FROM {DB_TABLE_NAME}")
                result = connection.execute(query)
                count = result.scalar()
                if count > 0:
                    print(f"[MIGRAÇÃO] Tabela SQLite '{DB_TABLE_NAME}' já contém {count} linhas. Migração ignorada.")
                    return True
    except Exception as e:
        print(f"[MIGRAÇÃO] Erro ao verificar tabela SQLite ({e}). Tentando migrar...")
    df_csv = read_historico_from_csv()
    if df_csv.empty:
        print("[MIGRAÇÃO] CSV histórico vazio. Migração concluída (sem dados).")
        return True
    try:
        colunas_para_migrar = [col for col in COLUNAS_HISTORICO if col in df_csv.columns]
        df_csv_para_migrar = df_csv[colunas_para_migrar]
        df_csv_para_migrar.to_sql(DB_TABLE_NAME, engine, if_exists='replace', index=False)
        print(f"[MIGRAÇÃO] SUCESSO! {len(df_csv)} linhas transferidas do CSV para o SQLite.")
        with engine.connect() as connection:
            cols_tabela_db = [col['name'] for col in inspector.get_columns(DB_TABLE_NAME)]
            for col_esperada in COLUNAS_HISTORICO:
                if col_esperada not in cols_tabela_db:
                    print(f"[MIGRAÇÃO] Adicionando coluna faltante '{col_esperada}' ao DB.")
                    connection.execute(text(f'ALTER TABLE {DB_TABLE_NAME} ADD COLUMN "{col_esperada}" REAL'))
        return True
    except Exception as e:
        adicionar_log("GERAL", f"ERRO CRÍTICO na migração CSV->SQLite: {e}")
        print(f"ERRO CRÍTICO na migração CSV->SQLite: {e}")
        return False


def read_data_from_sqlite(id_ponto, start_dt, end_dt):
    print(f"[SQLite] Consultando dados para {id_ponto} de {start_dt} a {end_dt}")
    engine = get_engine()
    inspector = inspect(engine)
    if not inspector.has_table(DB_TABLE_NAME):
        print(f"[SQLite] Tabela '{DB_TABLE_NAME}' não existe. Retornando vazio.")
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
            # Garante que o timestamp lido do SQLite seja 'aware' (UTC)
            if df['timestamp'].dt.tz is None:
                df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
        for col in COLUNAS_HISTORICO:
            if col not in df.columns:
                df[col] = pd.NA
        return df[COLUNAS_HISTORICO]
    except Exception as e:
        print(f"ERRO CRÍTICO ao ler do SQLite: {e}")
        adicionar_log("GERAL", f"ERRO CRÍTICO ao ler do SQLite: {e}")
        return pd.DataFrame(columns=COLUNAS_HISTORICO)


# ==========================================================
# --- FUNÇÕES DE TEMPO DE SINCRONIA (NOVAS) ---
# ==========================================================

def save_last_api_update(timestamp_utc):
    """ Salva o timestamp da última busca bem-sucedida no disco. """
    try:
        # A API Plugfield retorna dados em UTC, garantimos que seja salvo como ISO.
        data = {"last_update_utc": timestamp_utc.isoformat()}
        with open(LAST_UPDATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f)
    except Exception as e:
        print(f"ERRO ao salvar last_api_update: {e}")


def get_last_api_update():
    """ Lê o timestamp da última busca no disco. Retorna datetime (aware-UTC) ou None. """
    try:
        with open(LAST_UPDATE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if 'last_update_utc' in data:
            # Garante que o timestamp retorne 'aware' (com fuso horário)
            return datetime.datetime.fromisoformat(data['last_update_utc']).replace(tzinfo=datetime.timezone.utc)
        return None
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    except Exception as e:
        print(f"ERRO ao ler last_api_update: {e}")
        return None


# ==========================================================
# --- FUNÇÕES DE LEITURA E ESCRITA CSV (Mantidas) ---
# ==========================================================
def read_historico_from_csv():
    try:
        colunas_para_ler = []
        if os.path.exists(HISTORICO_FILE_CSV):
            colunas_validas_no_csv = pd.read_csv(HISTORICO_FILE_CSV, nrows=0).columns.tolist()
            colunas_para_ler = [col for col in COLUNAS_HISTORICO if col in colunas_validas_no_csv]
            if not colunas_para_ler:
                colunas_para_ler = None
        else:
            raise FileNotFoundError
        historico_df = pd.read_csv(HISTORICO_FILE_CSV, sep=',', usecols=colunas_para_ler)
        if 'timestamp' in historico_df.columns:
            historico_df['timestamp'] = pd.to_datetime(historico_df['timestamp'], utc=True)
        for col in COLUNAS_HISTORICO:
            if col not in historico_df.columns:
                historico_df[col] = pd.NA
        print(f"[CSV] Histórico lido: {len(historico_df)} entradas.")
        return historico_df
    except FileNotFoundError:
        print(f"[CSV] Arquivo '{HISTORICO_FILE_CSV}' não encontrado. Criando novo.")
        return pd.DataFrame(columns=COLUNAS_HISTORICO)
    except Exception as e:
        adicionar_log("CSV_READ", f"ERRO ao ler {HISTORICO_FILE_CSV}: {e}")
        print(f"ERRO ao ler {HISTORICO_FILE_CSV}: {e}")
        return pd.DataFrame(columns=COLUNAS_HISTORICO)


def save_historico_to_csv(df):
    try:
        df_sem_duplicatas = df.sort_values(by='timestamp').drop_duplicates(
            subset=['id_ponto', 'timestamp'], keep='last')
        max_pontos = MAX_HISTORICO_PONTOS * len(PONTOS_DE_ANALISE)
        df_truncado = df_sem_duplicatas.tail(max_pontos)
        colunas_para_salvar = [col for col in COLUNAS_HISTORICO if col in df_truncado.columns]
        df_truncado[colunas_para_salvar].to_csv(HISTORICO_FILE_CSV, index=False)
        print(f"[CSV] Histórico salvo no arquivo (Mantidas {len(df_truncado)} entradas).")
    except Exception as e:
        adicionar_log("CSV_SAVE", f"ERRO ao salvar histórico: {e}")
        print(f"ERRO ao salvar CSV: {e}")


# ==========================================================
# --- FUNÇÃO DE LEITURA PRINCIPAL PARA O DASHBOARD (Mantida) ---
# ==========================================================
def get_all_data_from_disk(worker_mode=False):
    """
    Dashboard (worker_mode=False) lê 14 dias do SQLITE.
    Worker (worker_mode=True) lê 72h do CSV.
    """

    if worker_mode:
        historico_df = read_historico_from_csv()
    else:
        print("[Dashboard] Lendo dados para o dashboard A PARTIR DO SQLITE (Últimos 14 dias)...")
        agora_utc = datetime.datetime.now(datetime.timezone.utc)
        end_dt = agora_utc
        start_dt = agora_utc - datetime.timedelta(days=14)
        lista_dfs = []
        for id_ponto in PONTOS_DE_ANALISE.keys():
            df_ponto = read_data_from_sqlite(id_ponto, start_dt, end_dt)
            if not df_ponto.empty:
                lista_dfs.append(df_ponto)
        if lista_dfs:
            historico_df = pd.concat(lista_dfs, ignore_index=True)
            print(f"[Dashboard] {len(historico_df)} registros lidos do SQLite para exibição.")
        else:
            print("[Dashboard] Nenhum dado encontrado no SQLite para o período.")
            historico_df = pd.DataFrame(columns=COLUNAS_HISTORICO)

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
def executar_passo_api_e_salvar(historico_df_csv):
    try:
        # A função fetch_and_process_plugfield_data está definida acima
        dados_api_df, logs_api = fetch_and_process_plugfield_data(historico_df_csv)
        status_novos = None
        for log in logs_api:
            mensagem_log_completa = f"| {log['id_ponto']} | {log['mensagem']}"
            print(mensagem_log_completa)
            adicionar_log(log['id_ponto'], log['mensagem'])
    except Exception as e:
        # O erro 'NameError' de antes foi resolvido movendo fetch_and_process_plugfield_data
        # para antes desta função.
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
        save_to_sqlite(dados_api_df)
        historico_atualizado_df_csv = pd.concat([historico_df_csv, dados_api_df], ignore_index=True)
        save_historico_to_csv(historico_atualizado_df_csv)

        # --- NOVO: Salva o timestamp da última atualização ---
        if 'timestamp' in dados_api_df.columns:
            # Pega o último timestamp do dado recebido da API
            ultimo_timestamp_api = dados_api_df['timestamp'].max().to_pydatetime().replace(tzinfo=datetime.timezone.utc)
            save_last_api_update(ultimo_timestamp_api)
            print(f"[SQLite] Último timestamp da API salvo: {ultimo_timestamp_api}")
        # --- FIM NOVO ---

        return dados_api_df, status_novos
    except Exception as e:
        adicionar_log("GERAL", f"ERRO CRÍTICO (processar/salvar): {e}")
        traceback.print_exc()
        return pd.DataFrame(), status_novos