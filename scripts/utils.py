import boto3
import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine
import datetime

# Configuração do MinIO
MINIO_ENDPOINT = "http://minio.heady:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"

# Configuração do PostgreSQL
DB_URL = "postgresql://heady:default@192.168.1.130:5432/biera_data_warehouse"

s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

def s3_get_files(bucket, subpasta, stream_name, year, month, day, tier="bronze"):
    """Lista arquivos no MinIO para uma determinada camada."""
    prefix = f"{subpasta}/{tier}/{stream_name}/{year}_{month}_{day}.csv"
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj["Key"] for obj in response.get("Contents", [])]

def s3_load_csv(bucket, object_key):
    """Carrega um CSV do MinIO em um DataFrame do Pandas."""
    response = s3_client.get_object(Bucket=bucket, Key=object_key)
    return pd.read_csv(BytesIO(response['Body'].read()))

def s3_upload_csv(df, bucket, subpasta, stream_name, year, month, day, tier="silver"):
    """Faz o upload de um DataFrame para o MinIO."""
    filename = f"{stream_name}_{tier}.csv"
    key = f"{subpasta}/{tier}/{filename}"
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer)
    print(f"Arquivo enviado: {key}")

def upload_to_postgres(df, schema, table_name):
    """Faz o upload de um DataFrame para o PostgreSQL."""
    engine = create_engine(DB_URL)
    df.to_sql(table_name, engine, if_exists='replace', schema=schema, index=False)
    print(f"Dados carregados: {schema}.{table_name}")
