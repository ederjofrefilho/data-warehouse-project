import pandas as pd
import datetime
import sys
import os
from sqlalchemy import create_engine

# Adiciona o caminho da pasta 'utils' ao PYTHONPATH
sys.path.append(os.path.join(os.path.dirname(__file__), '../utils'))
from minio_utils import get_files_for_stream, load_csv_from_minio, bucket_name, upload_to_silver, list_files_in_silver

# Obtém a data atual
today = datetime.datetime.now(datetime.UTC)

# Função para realizar o upload para o PostgreSQL, incluindo schema
def upload_to_postgres(df, table_name, schema, db_url):
    """Faz o upload de um DataFrame para uma tabela do PostgreSQL com schema"""
    # Cria uma engine de conexão com o PostgreSQL usando SQLAlchemy
    engine = create_engine(db_url)
    
    # Carrega o DataFrame para a tabela no PostgreSQL, se a tabela não existir ela será criada
    df.to_sql(table_name, engine, if_exists='replace', schema=schema, index=False)  # 'replace' pode ser alterado para 'append' para adicionar sem sobrescrever
    
    print(f"Dados carregados para o schema '{schema}' e tabela '{table_name}' no PostgreSQL.")

def campanhas_bronze_to_silver(year=today.strftime("%Y"), month=today.strftime("%m"), day=today.strftime("%d")):
    # Obtém os arquivos da pasta "meta-ads/campaigns" para a data especificada
    arquivos = get_files_for_stream("meta-ads", "campaigns", year, month, day)
    if not arquivos:
        print("Nenhum arquivo disponível para processamento.")
        return None
    
    # Carrega o primeiro arquivo encontrado
    df_novo = load_csv_from_minio(bucket_name, arquivos[0])
    
    # Reordenando as colunas
    colunas_ordenadas = [
        "id", "account_id", "name", "updated_time", "created_time", "start_time", "stop_time", 
        "status", "objective", "bid_strategy", "lifetime_budget", "daily_budget", "source_campaign_id"
    ]
    df_novo = df_novo[colunas_ordenadas]
    
    # Renomeando colunas
    renomear_colunas = {
        "id": "Campaign_ID", "account_id": "Account_ID", "name": "Campaign_Name", 
        "updated_time": "Updated_Time", "created_time": "Created_Time", "start_time": "Start_Time", 
        "stop_time": "Stop_Time", "status": "Status", "objective": "Objective", 
        "bid_strategy": "Bid_Strategy", "lifetime_budget": "Lifetime_Budget", 
        "daily_budget": "Daily_Budget", "source_campaign_id": "Source_Campaign_ID"
    }
    df_novo = df_novo.rename(columns=renomear_colunas)
    
    # Convertendo tipos de dados
    df_novo["Updated_Time"] = pd.to_datetime(df_novo["Updated_Time"], errors='coerce')
    df_novo["Created_Time"] = pd.to_datetime(df_novo["Created_Time"], errors='coerce')
    df_novo["Start_Time"] = pd.to_datetime(df_novo["Start_Time"], errors='coerce')
    df_novo["Stop_Time"] = pd.to_datetime(df_novo["Stop_Time"], errors='coerce')
    df_novo["Lifetime_Budget"] = pd.to_numeric(df_novo["Lifetime_Budget"], errors='coerce')
    df_novo["Daily_Budget"] = pd.to_numeric(df_novo["Daily_Budget"], errors='coerce')
    
    # Dividindo os valores de 'Lifetime_Budget' e 'Daily_Budget' por 100
    df_novo["Lifetime_Budget"] = df_novo["Lifetime_Budget"] / 100
    df_novo["Daily_Budget"] = df_novo["Daily_Budget"] / 100
    
    # Verifica se já existe algum arquivo na pasta "silver" para a data especificada
    files_in_silver = list_files_in_silver(bucket_name, "meta-ads", "campaigns", year, month, day)
    
    # Se existirem arquivos na pasta "silver", carregar o(s) existente(s)
    if files_in_silver:
        print("Arquivos existentes encontrados na pasta 'silver', agregando os dados.")
        
        # Carregar todos os arquivos existentes na pasta "silver"
        df_existente = pd.concat([load_csv_from_minio(bucket_name, file) for file in files_in_silver], ignore_index=True)
        
        # Agora, combinamos os dados existentes com os novos dados
        df_combinado = pd.concat([df_existente, df_novo], ignore_index=True)
    else:
        # Se não houver arquivos existentes, apenas usa o novo DataFrame
        df_combinado = df_novo
    
    # Remover duplicatas com base na coluna 'Campaign_ID' (pode ajustar se houver outra chave única)
    df_combinado = df_combinado.drop_duplicates(subset=["Campaign_ID"], keep="last")
    
    # Fazer o upload do DataFrame combinado para o MinIO na pasta "silver"
    upload_to_silver(df_combinado, subpasta="meta-ads", stream_name="campaigns", year=year, month=month, day=day)
    
    # Realizar o upload dos dados para o PostgreSQL (substitua com sua URL de conexão)
    db_url = "postgresql://heady:default@192.168.1.130:5432/biera_data_warehouse"  # Exemplo de URL
    schema = "meta ads"  # Defina o nome do schema desejado
    upload_to_postgres(df_combinado, "dim_Campaigns", schema, db_url)
    
    return df_combinado

# Exemplo de uso
df_transformado = campanhas_bronze_to_silver()
if df_transformado is not None:
    print(df_transformado.head())
