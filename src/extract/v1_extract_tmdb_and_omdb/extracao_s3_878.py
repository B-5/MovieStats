import os
import json
import requests
import boto3
from datetime import datetime

tmdb_api_key = os.environ.get('tmdb_api_key')
omdb_api_key = os.environ.get('omdb_api_key')
s3_bucket_name = 'data-lake-compass-etl'

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    processing_date = datetime.now().strftime('%Y_%m_%d')
    genre_id = 878  # Sci-Fi
    print("script started")

    movie_data = []
    page = 1
    total_pages = 1  # Apenas inicialização da variável.
    
    while page <= 3 and page <= total_pages: # Para testes. Apenas algumas páginas.
        # while page <= total_pages:
        url = f'https://api.themoviedb.org/3/discover/movie?api_key={tmdb_api_key}&with_genres={genre_id}&page={page}'
        response = requests.get(url)
        data = response.json()

        for movie in data['results']:
            tmdb_title = movie.get('title', None)
            tmdb_vote_average = movie.get('vote_average', None)
            tmdb_release_date = movie.get('release_date', None)
            # TMDB ID será utilizado para encontrar o IMDB ID.
            tmdb_id = movie.get('id', None)

            # Buscando o ID IMDB.
            tmdb_url = f'https://api.themoviedb.org/3/movie/{tmdb_id}/external_ids?api_key={tmdb_api_key}'
            tmdb_response = requests.get(tmdb_url)
            tmdb_data = tmdb_response.json()
            imdb_id = tmdb_data.get('imdb_id', None)

            if imdb_id:
                # Make a request to the OMDB API using the IMDB ID
                omdb_url = f'http://www.omdbapi.com/?i={imdb_id}&apikey={omdb_api_key}'
                omdb_response = requests.get(omdb_url)
                omdb_data = omdb_response.json()

                # Extract relevant OMDB information
                omdb_director = omdb_data.get('Director', None)
                omdb_box_office = omdb_data.get('BoxOffice', None)
                omdb_runtime = omdb_data.get('Runtime', None)
            else:
                omdb_director = None
                omdb_box_office = None
                omdb_runtime = None

            omdb_director = omdb_data.get('Director', None)
            omdb_box_office = omdb_data.get('BoxOffice', None)
            omdb_runtime = omdb_data.get('Runtime', None)

            movie_info = {
                'title': tmdb_title,
                'director': omdb_director,
                'box_office': omdb_box_office,
                'ratings': tmdb_vote_average,
                'genre': genre_id,
                'runtime': omdb_runtime,
                'year_of_publishing': tmdb_release_date[:4]
            }
            movie_data.append(movie_info)

        total_pages = data['total_pages']
        page += 1
        print(f"entering page {page}")

    batch_size = 100
    batches = [movie_data[i:i+batch_size]
               for i in range(0, len(movie_data), batch_size)]

    # Upload S3
    for i, batch in enumerate(batches):
        file_name = f'batch_{i+1}.json'
        s3.put_object(Bucket=s3_bucket_name,
                      Key=f'Raw/TMDB/JSON/{processing_date}/{file_name}', Body=json.dumps(batch))

    return {
        'statusCode': 200,
        'body': json.dumps('Data processing and upload complete!')
    }
