import boto3
import datetime
from io import BytesIO
import pandas as pd

# Configurar MinIO
minio_endpoint = "http://minio.heady:9000"
access_key = "minioadmin"
secret_key = "minioadmin"
bucket_name = "biera-datalake"

s3_client = boto3.client(
    "s3",
    endpoint_url=minio_endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

# Obtém a data atual
today = datetime.datetime.now(datetime.UTC)

def get_files_for_stream(subpasta, stream_name, year=today.strftime("%Y"), month = today.strftime("%m"), day = today.strftime("%d")):
    """Lista arquivos de um determinado stream no formato correto."""
    filename_pattern = f"{year}_{month}_{day}.csv"
    prefix = f"{subpasta}/bronze/{stream_name}/{filename_pattern}"
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    arquivos = []
    if "Contents" in response:
        arquivos = [obj["Key"] for obj in response["Contents"]]
        print(f"Arquivos encontrados para {stream_name}: {arquivos}")
    else:
        print(f"Nenhum arquivo encontrado para {stream_name}.")
    
    return arquivos

def load_csv_from_minio(bucket, object_key):
    """Baixa um arquivo CSV do MinIO e carrega no Pandas DataFrame."""
    response = s3_client.get_object(Bucket=bucket, Key=object_key)
    csv_content = response['Body'].read()
    return pd.read_csv(BytesIO(csv_content))

def list_files_in_silver(bucket, subpasta, stream_name, year=today.strftime("%Y"), month=today.strftime("%m"), day=today.strftime("%d")):
    """Lista arquivos na pasta 'silver' do MinIO para um determinado stream."""
    filename_pattern = f"{stream_name}_silver.csv"  # Arquivo de saída para a pasta "silver"
    prefix = f"{subpasta}/silver/{filename_pattern}"
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    arquivos = []
    if "Contents" in response:
        arquivos = [obj["Key"] for obj in response["Contents"]]
        print(f"Arquivos encontrados na pasta 'silver' para {stream_name}: {arquivos}")
    else:
        print(f"Nenhum arquivo encontrado na pasta 'silver' para {stream_name}.")
    
    return arquivos

def upload_to_silver(df, subpasta, stream_name, year=today.strftime("%Y"), month=today.strftime("%m"), day=today.strftime("%d")):
    """Faz o upload de um DataFrame para a pasta 'silver' no MinIO."""
    # Definindo o nome do arquivo para o diretório 'silver'
    filename = f"{stream_name}_silver.csv"
    key = f"{subpasta}/silver/{filename}"
    
    # Convertendo o DataFrame para CSV e carregando no MinIO
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    # Enviar o arquivo para o MinIO
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer)
    print(f"Arquivo enviado para o MinIO: {key}")