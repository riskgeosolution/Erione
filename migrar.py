# migrar.py (ATUALIZADO para Inclinômetros)

import pandas as pd
from sqlalchemy import create_engine, inspect, text
import data_source
import config
import os
import sys

print("--- INICIANDO MIGRAÇÃO MANUAL (CSV -> SQLite) ---")

try:
    data_source.setup_disk_paths()
except Exception as e:
    print(f"Erro ao configurar caminhos: {e}")
    sys.exit()

print(f"Lendo dados de: {data_source.HISTORICO_FILE_CSV}")
df_csv = data_source.read_historico_from_csv()

if df_csv.empty:
    print("ERRO: 'historico_temp.csv' está vazio ou não foi encontrado. Nada para migrar.")
else:
    print(f"Sucesso! {len(df_csv)} linhas lidas do CSV.")

    try:
        engine = data_source.get_engine()
        inspector = inspect(engine)

        print(f"Conectando ao SQLite em: {config.DB_CONNECTION_STRING}")
        print(f"Escrevendo {len(df_csv)} linhas na tabela: '{config.DB_TABLE_NAME}'...")

        # Pega só as colunas que o app espera
        colunas_para_migrar = [col for col in data_source.COLUNAS_HISTORICO if col in df_csv.columns]
        df_csv_para_migrar = df_csv[colunas_para_migrar]

        # Salva (replace)
        df_csv_para_migrar.to_sql(
            config.DB_TABLE_NAME,
            engine,
            if_exists='replace',
            index=False
        )

        print("Migração inicial concluída. Verificando colunas...")

        # --- INÍCIO DA ATUALIZAÇÃO ---
        # Adiciona colunas faltantes (ex: inclinometro) se o CSV for antigo
        with engine.connect() as connection:
            cols_tabela_db = [col['name'] for col in inspector.get_columns(config.DB_TABLE_NAME)]

            for col_esperada in data_source.COLUNAS_HISTORICO:
                if col_esperada not in cols_tabela_db:
                    print(f"[MIGRAÇÃO] Adicionando coluna faltante '{col_esperada}' ao DB.")
                    connection.execute(text(f'ALTER TABLE {config.DB_TABLE_NAME} ADD COLUMN "{col_esperada}" REAL'))

        # Adiciona a restrição UNIQUE (pode falhar se 'corrigir_db' já rodou, mas é seguro)
        try:
            with engine.connect() as connection:
                connection.execute(text(
                    f'CREATE UNIQUE INDEX IF NOT EXISTS idx_ponto_timestamp ON {config.DB_TABLE_NAME}(id_ponto, "timestamp")'))
                print("[MIGRAÇÃO] Restrição UNIQUE garantida.")
        except Exception as e_unique:
            print(f"[MIGRAÇÃO] Aviso ao criar índice UNIQUE (pode já existir): {e_unique}")
        # --- FIM DA ATUALIZAÇÃO ---

        print("\n--- MIGRAÇÃO CONCLUÍDA COM SUCESSO! ---")

    except Exception as e:
        print(f"\n--- ERRO NA MIGRAÇÃO ---")
        print(f"Ocorreu um erro: {e}")

print("-------------------------------------------------")