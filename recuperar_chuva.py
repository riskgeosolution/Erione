# recuperar_chuva.py (MODO SEGURO - VERIFICAÇÃO)
# Script para ler os dados do BANCO DE DADOS das últimas 6 horas e verificar se a chuva está zerada.

import pandas as pd
import datetime
import data_source
from sqlalchemy import text

def verificar_db_6h():
    print("--- VERIFICANDO DADOS NO BANCO DE DADOS (Últimas 6h) ---")
    
    # 1. Configura a conexão
    data_source.setup_disk_paths()
    engine = data_source.get_engine()
    
    # 2. Define o período
    agora_utc = datetime.datetime.now(datetime.timezone.utc)
    inicio_utc = agora_utc - datetime.timedelta(hours=6)
    
    print(f"Período: {inicio_utc.isoformat()} até {agora_utc.isoformat()}")
    
    # 3. Consulta SQL direta
    query = text(f"""
        SELECT "timestamp", "chuva_mm", "id_ponto"
        FROM {data_source.DB_TABLE_NAME}
        WHERE "timestamp" >= :start
        ORDER BY "timestamp" ASC
    """)
    
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(
                query, 
                connection, 
                params={"start": inicio_utc.strftime('%Y-%m-%d %H:%M:%S')}
            )
            
        if not df.empty:
            print(f"\nTotal de registros encontrados: {len(df)}")
            
            # Converte timestamp para datetime se necessário
            if 'timestamp' in df.columns:
                 df['timestamp'] = pd.to_datetime(df['timestamp'])

            # Filtra apenas chuva > 0
            df_chuva = df[df['chuva_mm'] > 0]
            
            print(f"Registros com chuva > 0: {len(df_chuva)}")
            print(f"Acumulado de chuva no período: {df['chuva_mm'].sum():.2f} mm")
            
            if not df_chuva.empty:
                print("\n--- AMOSTRA DE CHUVA > 0 NO DB ---")
                print(df_chuva.head(10))
            else:
                print("\n--- AMOSTRA DOS DADOS NO DB (ZERADOS) ---")
                print(df.tail(10))
                
        else:
            print("Nenhum dado encontrado no banco para este período.")
            
    except Exception as e:
        print(f"ERRO AO CONSULTAR DB: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verificar_db_6h()