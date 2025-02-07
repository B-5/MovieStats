from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

# Inicialização da sessão Spark.
spark = SparkSession.builder.appName("Refine").getOrCreate()

data = spark.read.parquet(
    "C:/Users/seysa/Downloads/part-00000-1dc023e3-9404-4120-85e0-fafc013eee9c-c000.snappy.parquet")

data.show()
data.describe().show()

# Filtragem das colunas relevantes.
filtered_data = data.filter(
    (col("Title").isNotNull()) &
    (col("Year").isNotNull()) &
    (col("Runtime").isNotNull()) &
    (col("Director").isNotNull()) &
    (col("Country").isNotNull()) &
    (col("BoxOffice").isNotNull()) &
    (col("imdbRating").isNotNull()) &
    (col("imdbVotes").isNotNull()) &
    (col("imdbID").isNotNull()) &
    (col("Language").isNotNull())
)

# Criação da tabela dimensão para diretor.
director_dim = filtered_data.select("Director").distinct(
).withColumn("DirectorId", monotonically_increasing_id())

# Criação da tabela fato para os filmes.
filme_fato = filtered_data.select(
    "imdbID", "Title", "Year", "Runtime", "Director", "Country", "BoxOffice", "imdbRating", "imdbVotes", "Language")

# Criação da surrogate key para Director.
filme_fato = filme_fato \
    .join(director_dim, filme_fato["Director"] == director_dim["Director"], "left") \
    .select(
        "Title", "Year", "Runtime", "Country", "BoxOffice", "imdbRating", "imdbVotes", "imdbID", "Language",
        director_dim["DirectorId"].alias("DirectorId"),
    )

filme_fato.show()
filme_fato.describe().show()
director_dim.show()
director_dim.describe().show()


spark.stop()
