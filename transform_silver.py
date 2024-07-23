import boto3
import duckdb
import os
import pandas as pd

# Crecenciais
bronze_bucket = 'bronze'
silver_bucket = 'silver'
filename = 'EXP_2024.parquet'
filename_parquet = 'EXP_2024_MG.parquet'

# Configura o client
s3 = boto3.client(
    's3',
    endpoint_url='https://bucket-production',  
    aws_access_key_id='dOW',
    aws_secret_access_key='0op'
)

# Baixa o arquivo do bucket 
response = s3.get_object(Bucket=bronze_bucket, Key=filename)

# Salva o arquivo 
with open(filename, 'wb') as f:
    f.write(response['Body'].read())
    
df = pd.read_parquet(filename)

# Processa o arquivo
try:
    # Conecta ao duck
    conn = duckdb.connect()
    
    # Cria a tabela 
    conn.execute(
        """
        CREATE TABLE data_transformacao AS
        SELECT * FROM df
        WHERE 'SG_UF_NCM' = '"MG"';
        """ 
    )
    
    # Salva a tabela como um arquivo Parquet
    conn.execute(f"""
        COPY (SELECT * FROM data_transformacao) TO '{filename_parquet}' (FORMAT PARQUET);
        """)
    
    # Envia o arquivo processado e transformado
    with open(filename_parquet, "rb") as file_data:
        s3.upload_fileobj(file_data, silver_bucket, filename_parquet)   
        print(f"Arquivo processado '{filename_parquet}' enviado para o bucket '{silver_bucket}' com sucesso.")
except Exception as e:
        print(f"Erro ao processar ou enviar o arquivo '{filename}': {e}")


# Lista de arquivos temporarios
temp_files = [filename, filename_parquet]

# Remove os arquivos
for file in temp_files:
    if os.path.exists(file):
        os.remove(file)