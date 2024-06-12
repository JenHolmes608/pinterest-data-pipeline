# Databricks notebook source
from pyspark.sql.functions import *
import urllib

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from functools import reduce

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

# AWS S3 bucket name
AWS_S3_BUCKET = "0afff69adbe3-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/mount_name"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
# dbutils.fs.mount(SOURCE_URL, MOUNT_NAME) 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/mount_name/topics/topics/1209b9ad90a5.pin/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_pin)

# COMMAND ----------

# 1. Replace empty entries and entries with no relevant data in each column with Nones
conditions = [
    ("", None),
    ("No description available Story format", None),
    ("User Info Error", None),
    ("Image src error.", None),
    ("N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", None),
    ("No Title Data Available", None)
]

# Function to apply multiple conditions
def apply_conditions(col, conditions):
    return reduce(lambda acc, cond: F.when(F.col(col) == cond[0], cond[1]).otherwise(acc), conditions, F.col(col))

# Apply the conditions to each column
df_pin = df_pin.select([apply_conditions(c, conditions).alias(c) for c in df_pin.columns])

# COMMAND ----------

# 2. Perform the necessary transformations on the follower_count to ensure every entry is a number and convert to int
# Remove non-numeric characters from follower_count and convert to integer
df_pin = df_pin.withColumn("follower_count", F.regexp_replace(F.col("follower_count"), "[^0-9]", ""))
df_pin = df_pin.withColumn("follower_count", F.col("follower_count").cast(IntegerType()))

# COMMAND ----------

# 3. Ensure that each column containing numeric data has a numeric data type
numeric_columns = ['follower_count', 'downloaded', 'index']
for col in numeric_columns:
    df_pin = df_pin.withColumn(col, F.col(col).cast(IntegerType()))

# COMMAND ----------

# 4. Clean the data in the save_location column to include only the save location path
df_pin = df_pin.withColumn("save_location", F.regexp_replace(F.col("save_location"), "^Local save in ", ""))

# COMMAND ----------

# 5. Rename the index column to ind
df_pin = df_pin.withColumnRenamed("index", "ind")

# COMMAND ----------

# 6. Reorder the DataFrame columns
df_pin = df_pin.select("ind", "unique_id", "title", "description", "follower_count",
                       "poster_name", "tag_list", "is_image_or_video", "image_src",
                       "save_location", "category")

# COMMAND ----------

display(df_pin)

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/mount_name/topics/topics/1209b9ad90a5.geo/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_geo)

# COMMAND ----------

# 1. Create a new column 'coordinates' that contains an array based on the 'latitude' and 'longitude' columns
df_geo = df_geo.withColumn("coordinates", F.array("latitude", "longitude"))

# COMMAND ----------

# 2. Drop the 'latitude' and 'longitude' columns from the DataFrame
df_geo = df_geo.drop("latitude", "longitude")

# COMMAND ----------

# 3. Convert the 'timestamp' column from a string to a timestamp data type
df_geo = df_geo.withColumn("timestamp", F.col("timestamp").cast("timestamp"))

# COMMAND ----------

# 4. Reorder the DataFrame columns to have the specified column order
df_geo = df_geo.select("ind", "country", "coordinates", "timestamp")

# COMMAND ----------

display(df_geo)

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/mount_name/topics/topics/1209b9ad90a5.user/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_user)

# COMMAND ----------

# 1. Create a new column 'user_name' that concatenates 'first_name' and 'last_name'
df_user = df_user.withColumn("user_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))

# COMMAND ----------

# 2. Drop the 'first_name' and 'last_name' columns from the DataFrame
df_user = df_user.drop("first_name", "last_name")

# COMMAND ----------

# 3. Convert the 'date_joined' column from a string to a timestamp data type
df_user = df_user.withColumn("date_joined", F.col("date_joined").cast("timestamp"))

# COMMAND ----------

# 4. Reorder the DataFrame columns to have the specified column order
df_user = df_user.select("ind", "user_name", "age", "date_joined")

# COMMAND ----------

display(df_user)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register the DataFrames as temporary views
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pin_view AS
# MAGIC SELECT ind, category
# MAGIC FROM df_pin;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW geo_view AS
# MAGIC SELECT ind, country
# MAGIC FROM df_geo;
# MAGIC
# MAGIC -- Join the pin and geo views on the common column 'ind'
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pin_geo_join AS
# MAGIC SELECT g.country, p.category
# MAGIC FROM pin_view p
# MAGIC JOIN geo_view g
# MAGIC ON p.ind = g.ind;
# MAGIC
# MAGIC -- Perform group-wise counting to find the most popular category in each country
# MAGIC CREATE OR REPLACE TEMPORARY VIEW popular_category_per_country AS
# MAGIC SELECT country, category, COUNT(*) AS category_count
# MAGIC FROM pin_geo_join
# MAGIC GROUP BY country, category;
# MAGIC
# MAGIC -- Rank the categories within each country based on category count
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ranked_categories_per_country AS
# MAGIC SELECT country, category, category_count,
# MAGIC        ROW_NUMBER() OVER (PARTITION BY country ORDER BY category_count DESC) AS category_rank
# MAGIC FROM popular_category_per_country;
# MAGIC
# MAGIC -- Filter the result to get only the most popular category in each country
# MAGIC CREATE OR REPLACE TEMPORARY VIEW most_popular_category_per_country AS
# MAGIC SELECT country, category, category_count
# MAGIC FROM ranked_categories_per_country
# MAGIC WHERE category_rank = 1;
# MAGIC
# MAGIC -- Convert the result to a DataFrame
# MAGIC SELECT * FROM most_popular_category_per_country;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register the DataFrames as temporary views
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pin_view AS
# MAGIC SELECT ind, category
# MAGIC FROM df_pin;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW geo_view AS
# MAGIC SELECT ind, timestamp
# MAGIC FROM df_geo;
# MAGIC
# MAGIC -- Join the pin and geo views on the common column 'ind'
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pin_geo_join AS
# MAGIC SELECT p.category, g.timestamp
# MAGIC FROM pin_view p
# MAGIC JOIN geo_view g
# MAGIC ON p.ind = g.ind;
# MAGIC
# MAGIC -- Filter the data for posts between 2018 and 2022
# MAGIC CREATE OR REPLACE TEMPORARY VIEW filtered_posts AS
# MAGIC SELECT category, timestamp
# MAGIC FROM pin_geo_join
# MAGIC WHERE YEAR(timestamp) BETWEEN 2018 AND 2022;
# MAGIC
# MAGIC -- Extract the year from the timestamp column
# MAGIC CREATE OR REPLACE TEMPORARY VIEW posts_with_year AS
# MAGIC SELECT category, YEAR(timestamp) AS post_year
# MAGIC FROM filtered_posts;
# MAGIC
# MAGIC -- Perform group-wise counting to find the number of posts for each category in each year
# MAGIC CREATE OR REPLACE TEMPORARY VIEW category_post_count AS
# MAGIC SELECT post_year, category, COUNT(*) AS category_count
# MAGIC FROM posts_with_year
# MAGIC GROUP BY post_year, category;
# MAGIC
# MAGIC -- Convert the result to a DataFrame
# MAGIC SELECT * FROM category_post_count
# MAGIC ORDER BY post_year;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register the DataFrames as temporary views
# MAGIC CREATE OR REPLACE TEMPORARY VIEW user_view AS
# MAGIC SELECT ind, user_name
# MAGIC FROM df_user;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW geo_view AS
# MAGIC SELECT ind, country
# MAGIC FROM df_geo;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pin_view AS
# MAGIC SELECT ind, poster_name, follower_count
# MAGIC FROM df_pin;
# MAGIC
# MAGIC -- Join the views on the common column 'ind'
# MAGIC CREATE OR REPLACE TEMPORARY VIEW join_view AS
# MAGIC SELECT u.user_name, g.country, p.poster_name, p.follower_count
# MAGIC FROM user_view u
# MAGIC JOIN geo_view g ON u.ind = g.ind
# MAGIC JOIN pin_view p ON p.ind = g.ind;
# MAGIC
# MAGIC -- Rank the users within each country based on follower_count
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ranked_users_per_country AS
# MAGIC SELECT country, user_name, poster_name, follower_count,
# MAGIC        ROW_NUMBER() OVER (PARTITION BY country ORDER BY follower_count DESC) AS user_rank
# MAGIC FROM join_view;
# MAGIC
# MAGIC -- Filter the result to get only the user with the most followers in each country
# MAGIC CREATE OR REPLACE TEMPORARY VIEW user_with_most_followers_per_country AS
# MAGIC SELECT country, poster_name, follower_count
# MAGIC FROM ranked_users_per_country
# MAGIC WHERE user_rank = 1;
# MAGIC
# MAGIC -- Convert the result to a DataFrame
# MAGIC SELECT * FROM user_with_most_followers_per_country;
# MAGIC
# MAGIC -- Perform group-wise counting to find the user with the most followers in each country
# MAGIC CREATE OR REPLACE TEMPORARY VIEW country_with_user_most_followers AS
# MAGIC SELECT country, MAX(follower_count) AS max_follower_count
# MAGIC FROM user_with_most_followers_per_country
# MAGIC GROUP BY country;
# MAGIC
# MAGIC -- Find the country with the user with the most followers
# MAGIC CREATE OR REPLACE TEMPORARY VIEW country_with_most_followers AS
# MAGIC SELECT c.country, u.poster_name, u.follower_count
# MAGIC FROM user_with_most_followers_per_country u
# MAGIC JOIN country_with_user_most_followers c
# MAGIC ON u.country = c.country AND u.follower_count = c.max_follower_count;
# MAGIC
# MAGIC -- Convert the result to a DataFrame
# MAGIC SELECT * FROM country_with_most_followers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register the DataFrames as temporary views
# MAGIC CREATE OR REPLACE TEMPORARY VIEW user_view AS
# MAGIC SELECT ind, user_name
# MAGIC FROM df_user;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW geo_view AS
# MAGIC SELECT ind, country
# MAGIC FROM df_geo;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pin_view AS
# MAGIC SELECT ind, poster_name, follower_count
# MAGIC FROM df_pin;
# MAGIC
# MAGIC -- Join the views on the common column 'ind'
# MAGIC CREATE OR REPLACE TEMPORARY VIEW join_view AS
# MAGIC SELECT u.user_name, g.country, p.poster_name, p.follower_count
# MAGIC FROM user_view u
# MAGIC JOIN geo_view g ON u.ind = g.ind
# MAGIC JOIN pin_view p ON p.ind = g.ind;
# MAGIC
# MAGIC -- Rank the users within each country based on follower_count
# MAGIC CREATE OR REPLACE TEMPORARY VIEW ranked_users_per_country AS
# MAGIC SELECT country, user_name, poster_name, follower_count,
# MAGIC        ROW_NUMBER() OVER (PARTITION BY country ORDER BY follower_count DESC) AS user_rank
# MAGIC FROM join_view;
# MAGIC
# MAGIC -- Filter the result to get only the user with the most followers in each country
# MAGIC CREATE OR REPLACE TEMPORARY VIEW user_with_most_followers_per_country AS
# MAGIC SELECT country, poster_name, follower_count
# MAGIC FROM ranked_users_per_country
# MAGIC WHERE user_rank = 1;
# MAGIC
# MAGIC -- Perform group-wise counting to find the user with the most followers in each country
# MAGIC CREATE OR REPLACE TEMPORARY VIEW country_with_user_most_followers AS
# MAGIC SELECT country, MAX(follower_count) AS max_follower_count
# MAGIC FROM user_with_most_followers_per_country
# MAGIC GROUP BY country;
# MAGIC
# MAGIC -- Find the country with the user with the most followers
# MAGIC CREATE OR REPLACE TEMPORARY VIEW country_with_most_followers AS
# MAGIC SELECT c.country, u.follower_count
# MAGIC FROM user_with_most_followers_per_country u
# MAGIC JOIN country_with_user_most_followers c
# MAGIC ON u.country = c.country AND u.follower_count = c.max_follower_count;
# MAGIC
# MAGIC -- Select the country with the highest number of followers
# MAGIC CREATE OR REPLACE TEMPORARY VIEW country_with_highest_followers AS
# MAGIC SELECT country, follower_count
# MAGIC FROM country_with_most_followers
# MAGIC ORDER BY follower_count DESC
# MAGIC LIMIT 1;
# MAGIC
# MAGIC -- Convert the result to a DataFrame containing only the desired columns
# MAGIC SELECT country, follower_count FROM country_with_highest_followers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register the DataFrames as temporary views
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pin_view AS
# MAGIC SELECT ind, category
# MAGIC FROM df_pin;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW user_view AS
# MAGIC SELECT ind, age
# MAGIC FROM df_user;
# MAGIC
# MAGIC -- Join the views on the common column 'ind'
# MAGIC CREATE OR REPLACE TEMPORARY VIEW join_view AS
# MAGIC SELECT p.category, u.age
# MAGIC FROM pin_view p
# MAGIC JOIN user_view u
# MAGIC ON p.ind = u.ind;
# MAGIC
# MAGIC -- Create age groups based on the age column
# MAGIC CREATE OR REPLACE TEMPORARY VIEW age_groups AS
# MAGIC SELECT *,
# MAGIC        CASE 
# MAGIC            WHEN age >= 18 AND age <= 24 THEN '18-24'
# MAGIC            WHEN age >= 25 AND age <= 35 THEN '25-35'
# MAGIC            WHEN age >= 36 AND age <= 50 THEN '36-50'
# MAGIC            WHEN age >= 51 THEN '+50'
# MAGIC        END AS age_group
# MAGIC FROM join_view;
# MAGIC
# MAGIC -- Perform group-wise counting to find the most popular category in each age group
# MAGIC CREATE OR REPLACE TEMPORARY VIEW popular_category_by_age_group AS
# MAGIC SELECT age_group, category, COUNT(*) AS category_count
# MAGIC FROM age_groups
# MAGIC GROUP BY age_group, category;
# MAGIC
# MAGIC -- Select the most popular category in each age group
# MAGIC CREATE OR REPLACE TEMPORARY VIEW most_popular_category_per_age_group AS
# MAGIC SELECT age_group, category, category_count,
# MAGIC        ROW_NUMBER() OVER (PARTITION BY age_group ORDER BY category_count DESC) AS category_rank
# MAGIC FROM popular_category_by_age_group;
# MAGIC
# MAGIC -- Filter the result to get only the most popular category in each age group
# MAGIC CREATE OR REPLACE TEMPORARY VIEW result AS
# MAGIC SELECT age_group, category, category_count
# MAGIC FROM most_popular_category_per_age_group
# MAGIC WHERE category_rank = 1;
# MAGIC
# MAGIC -- Convert the result to a DataFrame
# MAGIC SELECT * FROM result;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register the DataFrames as temporary views
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pin_view AS
# MAGIC SELECT ind, follower_count
# MAGIC FROM df_pin;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW user_view AS
# MAGIC SELECT ind, age
# MAGIC FROM df_user;
# MAGIC
# MAGIC -- Join the views on the common column 'ind'
# MAGIC CREATE OR REPLACE TEMPORARY VIEW join_view AS
# MAGIC SELECT p.follower_count, u.age
# MAGIC FROM pin_view p
# MAGIC JOIN user_view u
# MAGIC ON p.ind = u.ind;
# MAGIC
# MAGIC -- Create age groups based on the age column
# MAGIC CREATE OR REPLACE TEMPORARY VIEW age_groups AS
# MAGIC SELECT *,
# MAGIC        CASE 
# MAGIC            WHEN age >= 18 AND age <= 24 THEN '18-24'
# MAGIC            WHEN age >= 25 AND age <= 35 THEN '25-35'
# MAGIC            WHEN age >= 36 AND age <= 50 THEN '36-50'
# MAGIC            ELSE '+50'
# MAGIC        END AS age_group
# MAGIC FROM join_view;
# MAGIC
# MAGIC -- Calculate the median follower count for each age group
# MAGIC CREATE OR REPLACE TEMPORARY VIEW median_follower_count_per_age_group AS
# MAGIC SELECT age_group,
# MAGIC        percentile_approx(follower_count, 0.5) AS median_follower_count
# MAGIC FROM age_groups
# MAGIC GROUP BY age_group;
# MAGIC
# MAGIC -- Convert the result to a DataFrame
# MAGIC SELECT * FROM median_follower_count_per_age_group;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register the DataFrame as a temporary view
# MAGIC CREATE OR REPLACE TEMPORARY VIEW user_view AS
# MAGIC SELECT date_joined
# MAGIC FROM df_user;
# MAGIC
# MAGIC -- Extract the year from the date_joined column and filter for users who joined between 2015 and 2020
# MAGIC CREATE OR REPLACE TEMPORARY VIEW filtered_users AS
# MAGIC SELECT YEAR(date_joined) AS join_year
# MAGIC FROM user_view
# MAGIC WHERE YEAR(date_joined) BETWEEN 2015 AND 2020;
# MAGIC
# MAGIC -- Count the number of users who joined each year
# MAGIC CREATE OR REPLACE TEMPORARY VIEW users_joined_per_year AS
# MAGIC SELECT join_year, COUNT(*) AS number_users_joined
# MAGIC FROM filtered_users
# MAGIC GROUP BY join_year;
# MAGIC
# MAGIC -- Convert the result to a DataFrame
# MAGIC SELECT * FROM users_joined_per_year;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register the DataFrames as temporary views
# MAGIC CREATE OR REPLACE TEMPORARY VIEW user_view AS
# MAGIC SELECT ind, date_joined
# MAGIC FROM df_user;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pin_view AS
# MAGIC SELECT ind, follower_count
# MAGIC FROM df_pin;
# MAGIC
# MAGIC -- Join the views on the common column 'ind'
# MAGIC CREATE OR REPLACE TEMPORARY VIEW join_view AS
# MAGIC SELECT p.follower_count, u.date_joined
# MAGIC FROM pin_view p
# MAGIC JOIN user_view u
# MAGIC ON p.ind = u.ind;
# MAGIC
# MAGIC -- Filter the data to include only users who joined between 2015 and 2020
# MAGIC CREATE OR REPLACE TEMPORARY VIEW filtered_users AS
# MAGIC SELECT follower_count, YEAR(date_joined) AS join_year
# MAGIC FROM join_view
# MAGIC WHERE YEAR(date_joined) BETWEEN 2015 AND 2020;
# MAGIC
# MAGIC -- Calculate the median follower count for users who joined between 2015 and 2020
# MAGIC CREATE OR REPLACE TEMPORARY VIEW median_follower_count AS
# MAGIC SELECT join_year,
# MAGIC        percentile_approx(follower_count, 0.5) AS median_follower_count
# MAGIC FROM filtered_users
# MAGIC GROUP BY join_year;
# MAGIC
# MAGIC -- Convert the result to a DataFrame
# MAGIC SELECT * FROM median_follower_count;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register the DataFrames as temporary views
# MAGIC CREATE OR REPLACE TEMPORARY VIEW user_view AS
# MAGIC SELECT ind, date_joined, age
# MAGIC FROM df_user;
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW pin_view AS
# MAGIC SELECT ind, follower_count
# MAGIC FROM df_pin;
# MAGIC
# MAGIC -- Join the views on the common column 'ind'
# MAGIC CREATE OR REPLACE TEMPORARY VIEW join_view AS
# MAGIC SELECT p.follower_count, u.date_joined, u.age
# MAGIC FROM pin_view p
# MAGIC JOIN user_view u
# MAGIC ON p.ind = u.ind;
# MAGIC
# MAGIC -- Filter the data to include only users who joined between 2015 and 2020
# MAGIC CREATE OR REPLACE TEMPORARY VIEW filtered_users AS
# MAGIC SELECT follower_count, YEAR(date_joined) AS join_year, age
# MAGIC FROM join_view
# MAGIC WHERE YEAR(date_joined) BETWEEN 2015 AND 2020;
# MAGIC
# MAGIC -- Create age groups based on the age column
# MAGIC CREATE OR REPLACE TEMPORARY VIEW age_groups AS
# MAGIC SELECT *,
# MAGIC        CASE 
# MAGIC            WHEN age >= 18 AND age <= 24 THEN '18-24'
# MAGIC            WHEN age >= 25 AND age <= 35 THEN '25-35'
# MAGIC            WHEN age >= 36 AND age <= 50 THEN '36-50'
# MAGIC            ELSE '+50'
# MAGIC        END AS age_group
# MAGIC FROM filtered_users;
# MAGIC
# MAGIC -- Calculate the median follower count for each age group
# MAGIC CREATE OR REPLACE TEMPORARY VIEW median_follower_count_per_age_group AS
# MAGIC SELECT age_group, join_year,
# MAGIC        percentile_approx(follower_count, 0.5) AS median_follower_count
# MAGIC FROM age_groups
# MAGIC GROUP BY age_group, join_year;
# MAGIC
# MAGIC -- Convert the result to a DataFrame
# MAGIC SELECT * FROM median_follower_count_per_age_group;
# MAGIC
