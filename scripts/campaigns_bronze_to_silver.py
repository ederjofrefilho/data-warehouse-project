import sys
import os
import pandas as pd
import datetime
from utils import get_files, load_csv, upload_csv, upload_to_postgres

# Obtém a data atual
today = datetime.datetime.now(datetime.UTC)

def campanhas_bronze_to_silver(year=today.strftime("%Y"), month=today.strftime("%m"), day=today.strftime("%d")):
    subpasta = "meta-ads"
    stream_name = "campaigns"
    
    arquivos = get_files(subpasta, stream_name, year, month, day, tier="bronze")
    if not arquivos:
        print("Nenhum arquivo disponível para processamento.")
        return None
    
    df_novo = load_csv("biera-datalake", arquivos[0])
    
    colunas_ordenadas = [
        "id", "account_id", "name", "updated_time", "created_time", "start_time", "stop_time", 
        "status", "objective", "bid_strategy", "lifetime_budget", "daily_budget", "source_campaign_id"
    ]
    df_novo = df_novo[colunas_ordenadas]
    
    renomear_colunas = {
        "id": "Campaign_ID", "account_id": "Account_ID", "name": "Campaign_Name", 
        "updated_time": "Updated_Time", "created_time": "Created_Time", "start_time": "Start_Time", 
        "stop_time": "Stop_Time", "status": "Status", "objective": "Objective", 
        "bid_strategy": "Bid_Strategy", "lifetime_budget": "Lifetime_Budget", 
        "daily_budget": "Daily_Budget", "source_campaign_id": "Source_Campaign_ID"
    }
    df_novo = df_novo.rename(columns=renomear_colunas)
    
    for col in ["Updated_Time", "Created_Time", "Start_Time", "Stop_Time"]:
        df_novo[col] = pd.to_datetime(df_novo[col], errors='coerce')
    
    for col in ["Lifetime_Budget", "Daily_Budget"]:
        df_novo[col] = pd.to_numeric(df_novo[col], errors='coerce') / 100
    
    files_in_silver = get_files(subpasta, stream_name, year, month, day, tier="silver")
    if files_in_silver:
        df_existente = pd.concat([load_csv("biera-datalake", file) for file in files_in_silver], ignore_index=True)
        df_combinado = pd.concat([df_existente, df_novo], ignore_index=True)
    else:
        df_combinado = df_novo
    
    df_combinado = df_combinado.drop_duplicates(subset=["Campaign_ID"], keep="last")
    
    upload_csv(df_combinado, subpasta, stream_name, year, month, day, tier="silver")
    upload_to_postgres(df_combinado, "dim_Campaigns", "meta_ads")
    
    return df_combinado

# Execução do pipeline
df_transformado = campanhas_bronze_to_silver()
if df_transformado is not None:
    print(df_transformado.head())
