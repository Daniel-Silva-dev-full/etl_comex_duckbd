import requests
import os
import boto3
import duckdb

import warnings
warnings.filterwarnings('ignore')


# Credenciais
s3 = boto3.client(
    's3',
    endpoint_url='https://bucket-production',  
    aws_access_key_id='dO',
    aws_secret_access_key='0op'
)

bucket_name = "raw"  

# Urls para download
urls = [
    'https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_2024.csv',
    'https://balanca.economia.gov.br/balanca/bd/tabelas/VIA.csv'
]

print("Fazendo download e enviando arquivos para o minio...")

for url in urls:
    # pega o nome dos arquivos
    filename = os.path.basename(url)

    # Faz o download 
    response = requests.get(url, stream=True, verify=False)
    if response.status_code == 200:
        # Salva o arquivo 
        with open(filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=128):
                file.write(chunk)

        # Processando com duck 
        try:
            # Conecta ao duck
            conn = duckdb.connect()

            ## Cria a tabela e carrega 
            conn.execute(f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{filename}')")

            # Envia o arquivo processado
            with open(filename, "rb") as file_data:
                s3.upload_fileobj(file_data, bucket_name, filename)
            print(f"Arquivo '{filename}' enviado para o bucket '{bucket_name}' com sucesso.")
        except Exception as e:
            print(f"Erro ao processar arquivo '{filename}': {e}")

        # Remove o arquivo temporareo
        if os.path.exists(filename):
            os.remove(filename)
    else:
        print(f"Falha ao baixar '{filename}'.")
