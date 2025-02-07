import os
import json
import requests
import boto3
import uuid  # Para geração de nomes aleatórios pros arquivos.
from tqdm import tqdm as my_bar  # Para mostrar o progresso do loop.
from datetime import datetime

# Parâmetros e obtenção de variáveis de ambiente.
s3_bucket_name = 'data-lake-compass-etl'
omdb_api_key = os.environ.get('omdb_api_key')
start_index = os.environ.get('start_index')
batch_size = 100

# Função que separa a lista de filmes em lotes.
def split_into_batches(data, batch_size):
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]


def lambda_handler(event, context):
    print("Script iniciado.")  # Para testes.
    s3 = boto3.client('s3')
    processing_date = datetime.now().strftime("%Y/%m/%d")

    def fetch_movie_data(imdb_id):
        # Subdomínio private prioriza usuários da API paga (muito mais rápida).
        url = f'http://private.omdbapi.com/?i={imdb_id}&apikey={omdb_api_key}'
        response = requests.get(url)

        try:
            data = response.json()
            if data.get('Response', '').lower() == 'true':
                return data
        except json.JSONDecodeError:  # Alguns IDs não estão na OMDB.
            pass

        return None

    start = 12000
    limiter = 14303
    with open("C:/Users/seysa/OneDrive/Documentos/Estágio Compass/Repositorio-PB-CompassUOL/Sprint 8/Desafio ETL - Parte 2/v2 OMDB/imdb_ids.txt", 'r') as file:
        imdb_ids = [line.strip()
                    for line in file.readlines()][start:limiter]
    print(imdb_ids)

    movie_data = []
    count = 0
    for imdb_id in my_bar(imdb_ids, desc="Progress", total=14303):
        movie_info = fetch_movie_data(imdb_id)

        if movie_info:
            movie_data.append({
                'Title': movie_info.get('Title'),
                'Year': movie_info.get('Year'),
                'Runtime': movie_info.get('Runtime'),
                'Director': movie_info.get('Director'),
                'Country': movie_info.get('Country'),
                'BoxOffice': movie_info.get('BoxOffice'),
                'imdbRating': movie_info.get('imdbRating'),
                'imdbVotes': movie_info.get('imdbVotes'),
                'imdbID': movie_info.get('imdbID'),
                'Language': movie_info.get('Language'),
            })
            count += 1

    # Upload S3
    # Mostra quantos filmes foram obtidos com sucesso (para testes).
    print(f"A lista contém {count} filmes.")
    batches = list(split_into_batches(movie_data, batch_size))

    for batch in batches:
        # Gera um nome aleatório para o arquivo.
        file_name = str(uuid.uuid4()) + '.json'
        s3.put_object(Bucket=s3_bucket_name,
                      Key=f'Raw/OMDB/JSON/{processing_date}/{file_name}', Body=json.dumps(batch))
        # Mostra o nome do arquivo que foi escrito (para testes).
        print(f"Objeto {file_name} escrito.")

    return {
        'statusCode': 200,
        'body': json.dumps('Processamento e upload completos.')
    }
