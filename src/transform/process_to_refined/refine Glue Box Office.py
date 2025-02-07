import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, monotonically_increasing_id

# Inicialização do Spark e do Glue.
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Caminhos de entrada e saída definidos como parâmetros do job.
s3_input_path = args['S3_INPUT_PATH']
s3_output_path = args['S3_TARGET_PATH']

# Carrega os dados brutos do S3
data = spark.read.parquet(s3_input_path)

# Filtragem das colunas relevantes.
filtered_data = data.filter(
    (col("Title").isNotNull())      &
    (col("Year").isNotNull())       &
    (col("Runtime").isNotNull())    &
    (col("Director").isNotNull())   &
    (col("Country").isNotNull())    &
    (col("BoxOffice").isNotNull())  &
    # (col("imdbRating").isNotNull()) &
    # (col("imdbVotes").isNotNull())  &
    (col("imdbID").isNotNull())     &
    (col("Language").isNotNull())
)

# Criação da tabela dimensão para diretor.
director_dim_boxoffice = filtered_data.select("Director").distinct(
).withColumn("DirectorId", monotonically_increasing_id())

# Criação da tabela fato para os filmes.
filme_fato_boxoffice = filtered_data.select(
    "imdbID", "Title", "Year", "Runtime", "Director", "Country", "BoxOffice", "Language")

# Criação da surrogate key para Director.
filme_fato_boxoffice = filme_fato_boxoffice \
    .join(director_dim_boxoffice, filme_fato_boxoffice["Director"] == director_dim_boxoffice["Director"], "left") \
    .select(
        "Title", "Year", "Runtime", "Country", "BoxOffice", "imdbID", "Language",
        director_dim_boxoffice["DirectorId"].alias("DirectorId"),
    )

# Write director_dim_boxoffice to a subdirectory
director_dim_boxoffice_output = s3_output_path + "/director_dim_boxoffice"
director_dim_boxoffice.write.parquet(director_dim_boxoffice_output, mode="overwrite")

# Write filme_fato_boxoffice to a different subdirectory
filme_fato_boxoffice_output = s3_output_path + "/filme_fato_boxoffice"
filme_fato_boxoffice.write.parquet(filme_fato_boxoffice_output, mode="overwrite")

# Encerra a sessão Spark.
sc.stop()