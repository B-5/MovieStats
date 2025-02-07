from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType

# Inicialização do Spark.
spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

# Caminho para a pasta contendo os arquivos JSON brutos no HDFS
caminho_dos_arquivos_hdfs = "hdfs:///user/gianluca/brutos"

# Define manualmente o esquema para os dados JSON
schema = StructType(
    [
        StructField("Title", StringType(), True),
        StructField("Year", StringType(), True),
        StructField("Runtime", StringType(), True),
        StructField("Director", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("BoxOffice", StringType(), True),
    ]
)

# Use o esquema especificado ao ler os dados JSON
dados_brutos = spark.read.schema(schema).json(caminho_dos_arquivos_hdfs)
dados_brutos.show()

# Remover caracteres não numéricos
dados_limpos = dados_brutos.withColumn(
    "Runtime", regexp_replace(col("Runtime"), "[^0-9]", "")
)
dados_limpos = dados_limpos.withColumn(
    "BoxOffice", regexp_replace(col("BoxOffice"), "[^0-9]", "")
)

# Converter para inteiro
dados_limpos = dados_limpos.withColumn("Year", dados_limpos["Year"].cast("int"))
dados_limpos = dados_limpos.withColumn("Runtime", dados_limpos["Runtime"].cast("int"))
dados_limpos = dados_limpos.withColumn("BoxOffice", dados_limpos["BoxOffice"].cast("int"))

# Substituir string N/A por None
colunas = ["Title", "Year", "Runtime", "Director", "Country", "BoxOffice"]
for coluna in colunas:
    dados_limpos = dados_limpos.withColumn(
        coluna, when(col(coluna) == "N/A", None).otherwise(col(coluna))
    )

# Limpeza dos dados
dados_limpos = dados_limpos.dropDuplicates()  # Remove entradas duplicadas
# dados_limpos = dados_limpos.dropna("any")     # Remove linhas com valores ausentes

dados_limpos.show()
dados_limpos.describe().show()

particoes = 4
dados_limpos = dados_limpos.repartition(particoes)
# Salvar os dados limpos em formato Parquet no HDFS
dados_limpos.write.mode("overwrite").parquet("hdfs:///user/gianluca/limpos")

# Encerrar a sessão Spark
spark.stop()
