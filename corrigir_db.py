# corrigir_db.py (v3 - Agora sim!)

import data_source
import config
from sqlalchemy import text, inspect
import sys

print("--- INICIANDO BANCO DE DADOS LOCAL (app_state e historico) ---")

try:
    engine = data_source.get_engine()
    inspector = inspect(engine)

    # --- 1. Criar e popular a tabela 'app_state' ---
    DB_STATE_TABLE = "app_state"
    with engine.connect() as connection:
        print(f"Criando tabela '{DB_STATE_TABLE}' (se não existir)...")
        connection.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {DB_STATE_TABLE} (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """))

        print(f"Populando tabela '{DB_STATE_TABLE}' com valores padrão...")
        for key, value in data_source.VALORES_PADRAO_ESTADO.items():
            connection.execute(text(f"""
                INSERT INTO {DB_STATE_TABLE} (key, value)
                VALUES (:key, :value)
                ON CONFLICT(key) DO NOTHING;
            """), {"key": key, "value": value})

        connection.commit()  # Salva as mudanças na 'app_state'

    # --- 2. Criar a tabela 'historico_monitoramento' ---
    DB_TABLE_NAME = config.DB_TABLE_NAME
    if not inspector.has_table(DB_TABLE_NAME):
        print(f"Criando tabela '{DB_TABLE_NAME}' (se não existir)...")

        # Pega a definição das colunas do data_source
        colunas_db = {
            'timestamp': 'TIMESTAMP', 'id_ponto': 'TEXT', 'chuva_mm': 'REAL',
            'precipitacao_acumulada_mm': 'REAL', 'umidade_1m_perc': 'REAL',
            'umidade_2m_perc': 'REAL', 'umidade_3m_perc': 'REAL',
            'base_1m': 'REAL', 'base_2m': 'REAL', 'base_3m': 'REAL',
            'inclinometro_x': 'REAL', 'inclinometro_y': 'REAL'
        }

        # (Usando sintaxe SQLite para 'UNIQUE' que funciona localmente)
        create_query = f"CREATE TABLE {DB_TABLE_NAME} (\n"
        create_query += ",\n".join([f'"{col}" {tipo}' for col, tipo in colunas_db.items()])
        create_query += f",\nUNIQUE(id_ponto, \"timestamp\")\n);"

        with engine.connect() as connection:
            connection.execute(text(create_query))
            connection.commit()  # Salva a criação da 'historico_monitoramento'

        print(f"Tabela '{DB_TABLE_NAME}' criada.")
    else:
        print(f"Tabela '{DB_TABLE_NAME}' já existe.")

    print("\n--- Banco de dados inicializado com sucesso. ---")

except Exception as e:
    print(f"\n--- ERRO NA INICIALIZAÇÃO ---")
    print(f"Ocorreu um erro: {e}")
    sys.exit(1)