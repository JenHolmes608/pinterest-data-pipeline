# Databricks notebook source
import sys 
sys.path.append("/Workspace/Users/holmes_jennifer@ymail.com/pinterest-data-pipeline")

from pyspark.sql.types import *
from pyspark.sql.functions import *
import urllib

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from functools import reduce

from pinterest_batch_data import clean_df_pin, clean_df_geo, clean_df_user

# COMMAND ----------

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

pin_struct = StructType([
    StructField("category", StringType(), True),
    StructField("description", StringType(), True),
    StructField("downloaded", IntegerType(), True),
    StructField("follower_count", StringType(), True),
    StructField("image_src", StringType(), True),
    StructField("index", IntegerType(), True),
    StructField("is_image_or_video", StringType(), True),
    StructField("poster_name", StringType(), True),
    StructField("save_location", StringType(), True),
    StructField("tag_list", StringType(), True),
    StructField("title", StringType(), True),
    StructField("unique_id", StringType(), True)
])

df_pin = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0afff69adbe3-pin') \
.option('initialPosition','latest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.option("mergeSchema", "true")\
.load()

df_pin = df_pin.selectExpr("CAST(data AS STRING) jsonData")
df_pin = df_pin.select(from_json("jsonData", pin_struct).alias("data")).select("data.*")
df_pin = clean_df_pin(df_pin)

display(df_pin)

df_pin.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0afff69adbe3_pin_table")

# COMMAND ----------

geo_struct = StructType([
    StructField("ind", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("country", StringType(), True)
])

df_geo = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0afff69adbe3-geo') \
.option('initialPosition','latest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

df_geo = df_geo.selectExpr("CAST(data AS STRING) jsonData")
df_geo = df_geo.select(from_json("jsonData", geo_struct).alias("data")).select("data.*")
df_geo = clean_df_geo(df_geo)

display(df_geo)

df_geo.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0afff69adbe3_geo_table")

# COMMAND ----------

user_struct = StructType([
    StructField("age", IntegerType(), True),
    StructField("date_joined", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("ind", IntegerType(), True),
    StructField("last_name", StringType(), True)
])

df_user = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0afff69adbe3-user') \
.option('initialPosition','latest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

df_user = df_user.selectExpr("CAST(data AS STRING) jsonData")
df_user = df_user.select(from_json("jsonData", user_struct).alias("data")).select("data.*")
df_user = clean_df_user(df_user)

display(df_user)

df_user.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0afff69adbe3_user_table")