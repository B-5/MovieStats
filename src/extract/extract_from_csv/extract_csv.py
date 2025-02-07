import boto3

# Parâmetros S3
nome_do_bucket = "data-lake-compass-etl"
camada_de_armazenamento = 'Raw'
origem_do_dado = 'Local'
formato_do_dado = 'CSV'
data_de_processamento = '2023/08/30'
s3 = boto3.client('s3')

# Upload
def upload(path, destination):
    s3.upload_file(path, nome_do_bucket, destination)

files_info = [
    {'nome': 'movies.csv', 'especificacao_do_dado': 'Movies'},
    {'nome': 'series.csv', 'especificacao_do_dado': 'Series'}
]

for file_info in files_info:
    file_name = file_info['nome']
    especificacao_do_dado = file_info['especificacao_do_dado']
    file_path = file_name
    s3_path = f"{camada_de_armazenamento}/{origem_do_dado}/{formato_do_dado}/{especificacao_do_dado}/{data_de_processamento}/{file_name}"
    upload(file_path, s3_path)
    print(f"Arquivo {file_name} carregado no S3 em {s3_path}.")