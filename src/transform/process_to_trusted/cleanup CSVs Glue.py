import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

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

# Define manualmente o esquema para o CSV
schema = StructType([
    StructField("id", StringType(), True),
    StructField("tituloPincipal", StringType(), True),
    StructField("tituloOriginal", StringType(), True),
    StructField("anoLancamento", IntegerType(), True),
    StructField("tempoMinutos", IntegerType(), True),
    StructField("genero", StringType(), True),
    StructField("notaMedia", DoubleType(), True),
    StructField("numeroVotos", IntegerType(), True),
    StructField("generoArtista", StringType(), True),
    StructField("personagem", StringType(), True),
    StructField("nomeArtista", StringType(), True),
    StructField("anoNascimento", IntegerType(), True),
    StructField("anoFalecimento", IntegerType(), True),
    StructField("profissao", StringType(), True),
    StructField("titulosMaisConhecidos", StringType(), True)
])

# Carregue os dados brutos do CSV
dados_brutos = spark.read.option(
    "delimiter", "|").schema(schema).csv(s3_input_path)

# Filtra as linhas com gêneros "Sci-Fi" ou "Fantasy"
dados_filtrados = dados_brutos.filter(
    (col("genero").like("%Sci-Fi%")) | (col("genero").like("%Fantasy%")))

# Deduplica os dados
dados_deduplicados = dados_filtrados.dropDuplicates()

# Converte os campos numéricos para inteiros
numeric_columns = ["anoLancamento", "tempoMinutos",
                   "numeroVotos", "anoNascimento", "anoFalecimento"]
for column in numeric_columns:
    dados_deduplicados = dados_deduplicados.withColumn(
        column, col(column).cast(IntegerType()))

# Carregue os dados resultantes em um DataFrame
dados_resultantes = dados_deduplicados

# Foi especificado que o particionamento não deveria ser usado para o processamento dos CSVs.

# Escreve os dados resultantes no S3 em formato Parquet.
dados_resultantes.coalesce(1).write.parquet(s3_output_path, mode="overwrite")

# Encerra a sessão Spark.
sc.stop()
