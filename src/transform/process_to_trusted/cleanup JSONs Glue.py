import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, when, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType

# Inicialização do Spark e do Glue.
args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Caminhos de entrada e saída definidos como parâmetros do job.
s3_input_path = args['S3_INPUT_PATH']
s3_output_path = args['S3_TARGET_PATH']

# Definição do schema do JSON.
schema = StructType(
    [
        StructField("Title", StringType(), True),
        StructField("Year", StringType(), True),
        StructField("Runtime", StringType(), True),
        StructField("Director", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("BoxOffice", StringType(), True),
        StructField("imdbRating", StringType(), True),
        StructField("imdbVotes", StringType(), True),
        StructField("imdbID", StringType(), True),
        StructField("Language", StringType(), True),
    ]
)

# Carrega os dados brutos do S3
dados_brutos = spark.read.schema(schema).json(s3_input_path)

# Remove todos os caracteres não numéricos das colunas Runtime, BoxOffice, imdbID e imdbVotes.
dados_limpos = dados_brutos.withColumn(
    "Runtime", regexp_replace(col("Runtime"), "[^0-9]", "")
)
dados_limpos = dados_limpos.withColumn(
    "BoxOffice", regexp_replace(col("BoxOffice"), "[^0-9]", "")
)
dados_limpos = dados_limpos.withColumn(
    "imdbID", regexp_replace(col("imdbID"), "[^0-9]", "")
)
dados_limpos = dados_limpos.withColumn(
    "imdbVotes", regexp_replace(col("imdbVotes"), "[^0-9]", "")
)

# Converte as colunas Year, Runtime, BoxOffice, imdbVotes e imdbID para inteiro.
# A coluna imdbRating é convertida para float.
dados_limpos = dados_limpos.withColumn(
    "Year", dados_limpos["Year"].cast("int"))
dados_limpos = dados_limpos.withColumn(
    "Runtime", dados_limpos["Runtime"].cast("int"))
dados_limpos = dados_limpos.withColumn(
    "BoxOffice", dados_limpos["BoxOffice"].cast("int"))
dados_limpos = dados_limpos.withColumn(
    "imdbRating", dados_limpos["imdbRating"].cast("float"))
dados_limpos = dados_limpos.withColumn(
    "imdbVotes", dados_limpos["imdbVotes"].cast("int"))
dados_limpos = dados_limpos.withColumn(
    "imdbID", dados_limpos["imdbID"].cast("int"))

# Substitui a string N/A por None em todas as colunas.
colunas = ["Title", "Year", "Runtime", "Director", "Country", "BoxOffice", "imdbRating", "imdbVotes", "imdbID", "Language"]
for coluna in colunas:
    dados_limpos = dados_limpos.withColumn(
        coluna, when(col(coluna) == "N/A", None).otherwise(col(coluna))
    )

# Limpeza dos dados:
# Remove todas as entradas duplicadas.
dados_limpos = dados_limpos.dropDuplicates()

# Garante que só um arquivo será escrito.
dados_limpos = dados_limpos.coalesce(1)

# Escreve o arquivo.
dados_limpos.write.parquet(s3_output_path, mode="overwrite")

# Encerra a sessão Spark.
sc.stop()
