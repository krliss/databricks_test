# Databricks notebook source
# Check the contents in tables 
dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# COMMAND ----------

# Define file type
file_type = "csv"
# Whether the file has a header
first_row_is_header = "true"
# Delimiter used in the file
delimiter = ","
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/FileStore/tables/new_user_credentials.csv")

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.where(col('User name')=='knowit-cocreate').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='knowit-cocreate').select('Secret access key').collect()[0]['Secret access key']
# Encode the secret key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# AWS S3 bucket name
AWS_S3_BUCKET = "knowit-cocreate-training/vybuss"
# Mount name for the bucket
MOUNT_NAME = "/mnt/knowit-cocreate-training/vybuss"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

# Check if the AWS S3 bucket was mounted successfully
display(dbutils.fs.ls("/mnt/knowit-cocreate-training/vybuss"))


# COMMAND ----------

# File location and type
file_location = "/mnt/knowit-cocreate-training/vybuss/c4997434-8833-4546-84fa-bac7751ec4bc.csv"
new_location = "/mnt/knowit-cocreate-training/vybuss/edited.csv"
file_type = "csv"
# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","
# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.option("inferSchema", infer_schema) \
.option("header", first_row_is_header) \
.option("sep", delimiter) \
.option("quote", "\"") \
.option("escape", "\"") \
.csv(file_location)

display(df)

# COMMAND ----------

# Allow creating table using non-emply location
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
# Save table
df.write.format("parquet").saveAsTable('crypto_train')

# COMMAND ----------

# Remove the file if it was saved before
dbutils.fs.rm('/mnt/knowit-cocreate-training/demo_example', True)
# Save to the mounted S3 bucket
df.write.save(f'/mnt/knowit-cocreate-training/demo_example', format='csv')
# Check if the file was saved successfuly
display(dbutils.fs.ls("/mnt/knowit-cocreate-training/demo_example"))

# COMMAND ----------

# Unmount S3 bucket
dbutils.fs.unmount("/mnt/knowit-cocreate-training/")

# COMMAND ----------

dbutils.fs.ls("s3://knowit-cocreate-training/vybuss/")


# COMMAND ----------


