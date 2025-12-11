# data_source.py (LIMPO PARA PRODUÇÃO)

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
    DB_CONNECTION_STRING, # Importado do config.py
    DB_TABLE_NAME
)

# --- Constantes do arquivo (sem alterações) ---
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

VALORES_PADRAO_ESTADO = {
    "API_AUTO_ATIVADA": "False",
    "UMIDADE_BASE_1M": "26.8",
    "UMIDADE_BASE_2M": "16.2",
    "UMIDADE_BASE_3M": "13.4",
    "INCLINOMETRO_BASE_X": "-17.7",
    "INCLINOMETRO_BASE_Y": "8.3",
}

_MIGRATION_PERFORMED = False

def _check_and_migrate_schema(engine):
    global _MIGRATION_PERFORMED
    if _MIGRATION_PERFORMED:
        return
    try:
        inspector = inspect(engine)
        if not inspector.has_table(DB_TABLE_NAME):
            _init_historico_table(engine)
        current_columns = {col['name'] for col in inspector.get_columns(DB_TABLE_NAME)}
        expected_columns = set(COLUNAS_HISTORICO)
        missing_columns = expected_columns - current_columns
        if missing_columns:
            with engine.connect() as connection:
                for col_name in missing_columns:
                    connection.execute(text(f'ALTER TABLE {DB_TABLE_NAME} ADD COLUMN "{col_name}" REAL'))
                connection.commit()
        _MIGRATION_PERFORMED = True
    except Exception as e:
        print(f"ERRO CRÍTICO durante a migração automática do schema: {e}")
        _MIGRATION_PERFORMED = True

def _get_plugfield_token(id_ponto_fisico):
    global PLUGFIELD_TOKEN_CACHE
    if id_ponto_fisico in PLUGFIELD_TOKEN_CACHE:
        return PLUGFIELD_TOKEN_CACHE[id_ponto_fisico], None
    try:
        config = PLUGFIELD_CONFIG[id_ponto_fisico]
        BASE_URL = "https://prod-api.plugfield.com.br"
        login_url = f"{BASE_URL}/login"
        headers = {'x-api-key': config['API_KEY'], 'Content-Type': 'application/json'}
        login_data = {"username": config['USERNAME'], "password": config['PASSWORD']}
        with httpx.Client(timeout=20.0) as client:
            response = client.post(login_url, headers=headers, data=json.dumps(login_data))
        if response.status_code == 200:
            access_token = response.json().get("access_token")
            PLUGFIELD_TOKEN_CACHE[id_ponto_fisico] = access_token
            return access_token, None
        else:
            return None, f"ERRO API Plugfield: Falha no login ({response.status_code})."
    except Exception as e:
        return None, f"ERRO CRÍTICO (Plugfield Login): {e}"

def _fetch_plugfield_sensor_data(token, api_key, station_id, sensor_id, start_ms, end_ms, sensor_name_log=""):
    sensor_url = "https://prod-api.plugfield.com.br/data/sensor"
    headers = {'x-api-key': api_key, 'Authorization': token}
    params = {'device': station_id, 'sensor': sensor_id, 'time': start_ms, 'timeMax': end_ms}
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.get(sensor_url, headers=headers, params=params)
        if response.status_code == 200:
            dados = response.json().get('data', [])
            # Removido print de debug
            return dados, None
        elif response.status_code == 401:
            global PLUGFIELD_TOKEN_CACHE
            for key, value in list(PLUGFIELD_TOKEN_CACHE.items()):
                if value == token: del PLUGFIELD_TOKEN_CACHE[key]
            return [], "Token de acesso expirou (401)."
        else:
            return [], f"Erro ao buscar sensor {sensor_id} ({response.status_code})."
    except Exception as e:
        return [], f"Erro de conexão ao buscar sensor {sensor_id}: {e}"

def fetch_and_process_plugfield_data(df_historico_existente):
    logs_api = []
    lista_dataframes_finais = []
    DATA_INICIO_PADRAO = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=72)

    df_ponto_existente = df_historico_existente[df_historico_existente['id_ponto'] == 'Ponto-1']
    agora_utc = datetime.datetime.now(datetime.timezone.utc)

    if df_ponto_existente.empty:
        start_dt_utc = DATA_INICIO_PADRAO
    else:
        start_dt_utc = df_ponto_existente['timestamp'].max()
        if start_dt_utc < DATA_INICIO_PADRAO: start_dt_utc = DATA_INICIO_PADRAO
    
    start_dt_utc += datetime.timedelta(seconds=1)
    end_time_ms = int(agora_utc.timestamp() * 1000)
    start_time_ms = int(start_dt_utc.timestamp() * 1000)

    if start_time_ms >= end_time_ms:
        print(f"[API Plugfield] Ponto-1 já está atualizado.")
        return pd.DataFrame(), logs_api

    for id_ponto_fisico, config in PLUGFIELD_CONFIG.items():
        print(f"[API Plugfield] Iniciando coleta para: {id_ponto_fisico}")
        try:
            station_id = config["STATION_ID"]
            api_key = config["API_KEY"]
            sensor_map = PLUGFIELD_SENSOR_MAP[id_ponto_fisico]
        except KeyError:
            logs_api.append({"id_ponto": id_ponto_fisico, "mensagem": f"ERRO: Configuração não encontrada."})
            continue

        token, erro_token = _get_plugfield_token(id_ponto_fisico)
        if erro_token:
            logs_api.append({"id_ponto": id_ponto_fisico, "mensagem": erro_token})
            continue

        dados_brutos_sensores = {}
        for nome_app, sensor_id in sensor_map.items():
            dados, erro = _fetch_plugfield_sensor_data(token, api_key, station_id, sensor_id, start_time_ms, end_time_ms, sensor_name_log=nome_app)
            if erro: logs_api.append({"id_ponto": id_ponto_fisico, "mensagem": erro})
            if dados: dados_brutos_sensores[nome_app] = dados

        if not dados_brutos_sensores:
            print(f"[API Plugfield] Nenhum dado retornado para {id_ponto_fisico}.")
            continue

        lista_dfs_processados = []
        for nome_app, dados_brutos in dados_brutos_sensores.items():
            df_sensor = pd.DataFrame(dados_brutos)
            if df_sensor.empty: continue
            df_sensor = df_sensor[['time', 'valueFormatted']]
            df_sensor['timestamp'] = pd.to_datetime(df_sensor['time'], unit='ms', utc=True)
            df_sensor = df_sensor.set_index('timestamp').drop(columns=['time'])
            df_sensor = df_sensor.rename(columns={'valueFormatted': nome_app})
            df_sensor = df_sensor[~df_sensor.index.duplicated(keep='last')]
            lista_dfs_processados.append(df_sensor)

        if not lista_dfs_processados:
            print(f"[API Plugfield] Processamento não gerou DataFrames para {id_ponto_fisico}.")
            continue

        df_ponto_final = pd.concat(lista_dfs_processados, axis=1, join='outer')
        df_ponto_final = df_ponto_final.interpolate(method='time', limit_area='inside', limit=4)
        df_ponto_final = df_ponto_final.sort_index()
        
        lista_dataframes_finais.append(df_ponto_final.reset_index())

    if not lista_dataframes_finais:
        return pd.DataFrame(), logs_api

    # Unifica todos os dataframes coletados
    df_resultado_final = pd.concat(lista_dataframes_finais, ignore_index=True)
    df_resultado_final = df_resultado_final.rename(columns={'index': 'timestamp'})
    
    # Define o id_ponto para 'Ponto-1' para o resto da aplicação
    df_resultado_final['id_ponto'] = 'Ponto-1'

    # Garante que a coluna de chuva seja numérica e preenche valores ausentes com 0
    if 'chuva_mm' in df_resultado_final.columns:
        df_resultado_final['chuva_mm'] = pd.to_numeric(df_resultado_final['chuva_mm'], errors='coerce').fillna(0.0)
    
    # Garante que todas as colunas esperadas existam
    for col in COLUNAS_HISTORICO:
        if col not in df_resultado_final.columns:
            df_resultado_final[col] = pd.NA
    
    logs_api.append({"id_ponto": "GERAL", "mensagem": f"API: Sucesso. {len(df_resultado_final)} novos registros totais processados."})
    return df_resultado_final, logs_api

def adicionar_log(id_ponto, mensagem):
    try:
        log_entry = f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} | {id_ponto} | {mensagem}\n"
        with open(LOG_FILE, 'a', encoding='utf-8') as f: f.write(log_entry)
    except Exception as e:
        print(f"ERRO CRÍTICO ao escrever no log: {e}")

def setup_disk_paths():
    print(f"--- data_source.py (Modo PostgreSQL) ---\nCaminho do Disco de Dados (Temporário): .\nBanco de Dados (Escrita): {DB_CONNECTION_STRING}")

def get_engine():
    return create_engine(DB_CONNECTION_STRING)

def _init_historico_table(engine):
    try:
        colunas_db = {'timestamp': 'TIMESTAMP', 'id_ponto': 'TEXT', 'chuva_mm': 'REAL', 'precipitacao_acumulada_mm': 'REAL', 'umidade_1m_perc': 'REAL', 'umidade_2m_perc': 'REAL', 'umidade_3m_perc': 'REAL', 'base_1m': 'REAL', 'base_2m': 'REAL', 'base_3m': 'REAL', 'inclinometro_x': 'REAL', 'inclinometro_y': 'REAL'}
        create_query = f"CREATE TABLE {DB_TABLE_NAME} (\n" + ",\n".join([f'"{col}" {tipo}' for col, tipo in colunas_db.items()]) + f",\nUNIQUE(id_ponto, \"timestamp\")\n);"
        with engine.connect() as connection:
            connection.execute(text(create_query))
            connection.commit()
    except Exception as e:
        print(f"ERRO CRÍTICO ao criar tabela de histórico: {e}")

def save_to_db(df_novos_dados):
    if df_novos_dados.empty: return
    try:
        engine = get_engine()
        _check_and_migrate_schema(engine)
        
        # Garante que as colunas do DataFrame correspondem às da tabela
        inspector = inspect(engine)
        cols_tabela_db = [col['name'] for col in inspector.get_columns(DB_TABLE_NAME)]
        cols_para_salvar = [col for col in df_novos_dados.columns if col in cols_tabela_db]
        df_para_salvar = df_novos_dados[cols_para_salvar]

        df_para_salvar.to_sql(DB_TABLE_NAME, engine, if_exists='append', index=False, method=None)
        print(f"[DB] {len(df_para_salvar)} novos pontos salvos no DB.")
    except exc.IntegrityError:
        print(f"[DB] Aviso: Dados duplicados para este timestamp. Ignorando.")
    except Exception as e:
        print(f"ERRO CRÍTICO ao salvar no DB: {e}")

def read_data_from_db(id_ponto, start_dt, end_dt):
    engine = get_engine()
    query = text(f'SELECT * FROM {DB_TABLE_NAME} WHERE id_ponto = :ponto AND "timestamp" >= :start AND "timestamp" < :end ORDER BY "timestamp" ASC')
    try:
        _check_and_migrate_schema(engine)
        df = pd.read_sql_query(query, engine, params={"ponto": id_ponto, "start": start_dt, "end": end_dt}, parse_dates=["timestamp"])
        if not df.empty and 'timestamp' in df.columns and df['timestamp'].dt.tz is None:
            df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
        for col in COLUNAS_HISTORICO:
            if col not in df.columns: df[col] = pd.NA
        if 'chuva_mm' in df.columns:
            df['chuva_mm'] = pd.to_numeric(df['chuva_mm'], errors='coerce').fillna(0.0)
        return df[COLUNAS_HISTORICO]
    except Exception as e:
        print(f"ERRO CRÍTICO ao ler do DB: {e}")
        return pd.DataFrame(columns=COLUNAS_HISTORICO)

def get_all_data_from_disk(worker_mode=False):
    dias_para_buscar = 4
    print(f"[Dashboard] Lendo dados para o dashboard A PARTIR DO DB (Últimos {dias_para_buscar} dias)...")
    agora_utc = datetime.datetime.now(datetime.timezone.utc)
    start_dt = agora_utc - datetime.timedelta(days=dias_para_buscar)
    
    lista_dfs = [read_data_from_db(id_ponto, start_dt, agora_utc) for id_ponto in PONTOS_DE_ANALISE.keys()]
    historico_df = pd.concat(lista_dfs, ignore_index=True) if lista_dfs else pd.DataFrame(columns=COLUNAS_HISTORICO)

    if not historico_df.empty:
        print(f"[Dashboard] {len(historico_df)} registros lidos do DB para exibição.")
    else:
        print("[Dashboard] Nenhum dado encontrado no DB para o período.")
    
    if 'chuva_mm' in historico_df.columns:
        historico_df['chuva_mm'] = pd.to_numeric(historico_df['chuva_mm'], errors='coerce').fillna(0.0)
    
    try:
        with open(STATUS_FILE, 'r', encoding='utf-8') as f: status_atual = json.load(f)
    except:
        status_atual = {p: {"geral": "INDEFINIDO"} for p in PONTOS_DE_ANALISE.keys()}
    try:
        with open(LOG_FILE, 'r', encoding='utf-8') as f: logs = f.read()
    except:
        logs = "Nenhum evento registrado."
    return historico_df, status_atual, logs

def executar_passo_api_e_salvar(historico_df_existente):
    try:
        dados_api_df, logs_api = fetch_and_process_plugfield_data(historico_df_existente)
        for log in logs_api:
            adicionar_log(log['id_ponto'], log['mensagem'])
        if not dados_api_df.empty:
            save_to_db(dados_api_df)
        return dados_api_df, None
    except Exception as e:
        adicionar_log("GERAL", f"ERRO CRÍTICO (executar_passo_api_e_salvar): {e}")
        return pd.DataFrame(), None
        
def get_app_state(key):
    # Dummy function, a ser substituída pela implementação real se necessário
    return VALORES_PADRAO_ESTADO.get(key)
def set_app_state(key, value):
    # Dummy function
    pass