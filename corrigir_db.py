# corrigir_db.py (ATUALIZADO para Inclinômetros)

import pandas as pd
from sqlalchemy import create_engine, text
import data_source
import config
import os
import sys
import shutil

print("--- INICIANDO CORREÇÃO DE DUPLICATAS (v2) ---")
print("Este script preserva TODOS os dados históricos.")

try:
    data_source.setup_disk_paths()
except Exception as e:
    print(f"Erro ao configurar caminhos: {e}")
    sys.exit()

db_path = config.DB_CONNECTION_STRING.replace("sqlite:///", "")
if os.environ.get('RENDER'):
    db_path = f"{data_source.DATA_DIR}/{os.path.basename(db_path)}"

db_backup_path = db_path + ".backup"
db_shm = db_path + "-shm"
db_wal = db_path + "-wal"

if not os.path.exists(db_path):
    print(f"ERRO: Banco de dados '{db_path}' não encontrado. Nada para corrigir.")
    sys.exit()

print(f"Encontrado banco de dados: {db_path}")

try:
    engine = data_source.get_engine()

    print("Lendo TODO o histórico do banco de dados atual para a memória...")
    query = f"SELECT * FROM {config.DB_TABLE_NAME}"
    df_completo = pd.read_sql_query(query, engine, parse_dates=["timestamp"])

    if df_completo.empty:
        print("Aviso: O banco de dados está vazio. Saindo.")
        sys.exit()

    print(f"Total de {len(df_completo)} linhas lidas.")

    # Limpa duplicatas em memória
    df_limpo = df_completo.drop_duplicates(subset=['id_ponto', 'timestamp'], keep='last')
    num_removidas = len(df_completo) - len(df_limpo)
    if num_removidas > 0:
        print(f"LIMPEZA: {num_removidas} linhas duplicadas foram removidas em memória.")
    else:
        print("LIMPEZA: Nenhuma duplicata encontrada.")

    print(f"Total de {len(df_limpo)} linhas únicas restantes.")

    print("Desconectando do banco de dados antigo...")
    engine.dispose()

    print(f"Criando backup do banco antigo em: {db_backup_path}")
    shutil.copy2(db_path, db_backup_path)

    print("Apagando o banco de dados antigo...")
    if os.path.exists(db_path): os.remove(db_path)
    if os.path.exists(db_shm): os.remove(db_shm)
    if os.path.exists(db_wal): os.remove(db_wal)

    print("Conectando ao novo arquivo de banco de dados...")
    engine_novo = data_source.get_engine()

    # --- INÍCIO DA ATUALIZAÇÃO ---
    # Adicionadas colunas 'inclinometro_x' e 'inclinometro_y'
    create_table_sql = f"""
    CREATE TABLE {config.DB_TABLE_NAME} (
        "timestamp" TIMESTAMP,
        id_ponto TEXT,
        chuva_mm REAL,
        precipitacao_acumulada_mm REAL,
        umidade_1m_perc REAL,
        umidade_2m_perc REAL,
        umidade_3m_perc REAL,
        base_1m REAL,
        base_2m REAL,
        base_3m REAL,
        inclinometro_x REAL,
        inclinometro_y REAL,
        UNIQUE(id_ponto, "timestamp")
    );
    """
    # --- FIM DA ATUALIZAÇÃO ---

    print("Criando nova tabela no SQLite com a restrição UNIQUE...")
    with engine_novo.connect() as connection:
        connection.execute(text(create_table_sql))

    print(f"Salvando as {len(df_limpo)} linhas limpas no novo banco de dados...")

    # Garante que apenas colunas existentes sejam salvas
    colunas_db = [col['name'] for col in inspect(engine_novo).get_columns(config.DB_TABLE_NAME)]
    colunas_para_salvar = [col for col in df_limpo.columns if col in colunas_db]
    df_para_salvar = df_limpo[colunas_para_salvar]

    df_para_salvar.to_sql(
        config.DB_TABLE_NAME,
        engine_novo,
        if_exists='append',
        index=False
    )

    print("\n--- CORREÇÃO CONCLUÍDA COM SUCESSO! ---")

except Exception as e:
    print(f"\n--- ERRO NA CORREÇÃO ---")
    print(f"Ocorreu um erro: {e}")
    print("Seus dados originais estão salvos no arquivo .backup")

print("-------------------------------------------------")