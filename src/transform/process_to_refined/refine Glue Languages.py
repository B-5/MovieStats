import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, split, explode

# Initialization of Spark and Glue.
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Input and output paths defined as job parameters.
s3_input_path = args['S3_INPUT_PATH']
s3_output_path = args['S3_TARGET_PATH']

# Load raw data from S3.
data = spark.read.parquet(s3_input_path)

# Filter relevant columns.
filtered_data = data.filter(
    (col("Title").isNotNull())      &
    (col("Labguage").isNotNull())
)

# Split the "Language" field into separate rows.
split_data = filtered_data.select(
    "Title",
    explode(split(col("Language"), ", ")).alias("Language")
)

# Write split_data to a subdirectory.
split_data_output = s3_output_path + "/split_data_language"
split_data.write.parquet(split_data_output, mode="overwrite")

# Close the Spark session.
sc.stop()