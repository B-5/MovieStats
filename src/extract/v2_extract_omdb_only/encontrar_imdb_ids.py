import csv

# Open the CSV file for reading
with open('C:/Users/seysa/OneDrive/Documentos/Estágio Compass/Repositorio-PB-CompassUOL/Sprint 7/Desafio ETL - Parte 1/Projeto/movies.csv', mode='r', encoding='utf-8') as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter='|')

    # Create a set to store unique IMDb IDs
    imdb_ids = set()

    # Iterate through each row in the CSV file
    for row in csv_reader:
        # Check if the 'genero' column contains 'Fantasy' or 'Sci-Fi'
        genres = row['genero'].split(',')
        if 'Fantasy' in genres or 'Sci-Fi' in genres:
            # Add the IMDb ID to the set
            imdb_ids.add(row['id'])

# Write the IMDb IDs to a file named 'imdb_ids.txt'
with open('imdb_ids.txt', mode='w', encoding='utf-8') as output_file:
    for imdb_id in imdb_ids:
        output_file.write(imdb_id + '\n')