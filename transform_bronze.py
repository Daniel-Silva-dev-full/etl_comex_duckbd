import boto3
import pandas as pd
import duckdb
import io
import os

# Credenciais
raw_bucket = 'raw'
bronze_bucket = 'bronze'
filename = 'EXP_2024.csv'
filename_parquet = 'EXP_2024.parquet'

# Configura o client
s3 = boto3.client(
    's3',
    endpoint_url='https://bucket-production',  
    aws_access_key_id='dOW',
    aws_secret_access_key='0op'
)

# Baixa o arquivo do bucket
response = s3.get_object(Bucket=raw_bucket, Key=filename)
csv_data = response['Body'].read().decode('utf-8')

df = pd.read_csv(io.StringIO(csv_data))

# Processa o arquivo 
try:
    # Conecta ao duck
    conn = duckdb.connect()
    
    # Cria a tabela e faz o clean
    conn.execute(
        """
        CREATE TABLE data_limpeza AS
        SELECT DISTINCT *
        FROM df
        WHERE 'QT_ESTAT' IS NOT NULL;
        """ 
    )
    
    # Salva a tabela como Parquet
    conn.execute(f"""
        COPY (SELECT * FROM data_limpeza) TO '{filename_parquet}' (FORMAT PARQUET);
        """)
    
    # Envia o arquivo processado 
    with open(filename_parquet, "rb") as file_data:
        s3.upload_fileobj(file_data, bronze_bucket, filename_parquet)
        print(f"Arquivo processado '{filename_parquet}' enviado para o bucket '{bronze_bucket}' com sucesso.")
except Exception as e:
        print(f"Erro ao processar ou enviar o arquivo '{filename}': {e}")


# Lista de arquivos temporarios
temp_files = [filename, filename_parquet]
# Remove os arquivos 
for file in temp_files:
    if os.path.exists(file):
        os.remove(file)
