from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Inicialize a sessão Spark
spark = SparkSession.builder.appName("CSVDataProcessing").getOrCreate()

# Caminho para a pasta contendo o arquivos CSV
caminho_dos_arquivos = "C:/Users/seysa/OneDrive/Documentos/Estágio Compass/Repositorio-PB-CompassUOL/Sprint 7/Desafio ETL - Parte 1/Projeto/movies.csv"

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
dados_brutos = spark.read.option("delimiter", "|").schema(
    schema).csv(caminho_dos_arquivos)

dados_brutos.describe().show()

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

# Exiba o DataFrame resultante
dados_resultantes.show()
dados_resultantes.describe().show()

# Salvar os dados limpos em formato Parquet
# dados_resultantes.write.mode("overwrite").parquet("C:/Users/seysa/OneDrive/Documentos/Estágio Compass/Repositorio-PB-CompassUOL/Sprint 9/Desafio ETL - Parte 3/Para Trusted/limposCSV")

# Encerre a sessão Spark
spark.stop()
