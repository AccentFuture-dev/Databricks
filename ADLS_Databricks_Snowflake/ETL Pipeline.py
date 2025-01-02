# Mount ADLS to Databricks
configs = {
    "fs.azure.account.key.developmentrawdata.blob.core.windows.net": 
"UWXic3OBQFiR3QSl2tDzG8tX3p0Ug2313fsqfasfqfqerqe311ZNA=="
}

dbutils.fs.mount(
    source="wasbs://raw@developmentrawdata.blob.core.windows.net/",
    mount_point="/mnt/raw_data",
    extra_configs=configs
)

# Verify Mount location

dbutils.fs.ls("/mnt/raw_data")

# Read data from databricks mounted location to spark data frame 

raw_data = spark.read.csv("/mnt/raw_data/raw_data.csv", header=True, 
inferSchema=True)
raw_data.show()

# Transform data

transformed_data = raw_data.filter(raw_data['age'] > 18).withColumnRenamed("age", "user_age")
transformed_data.show()

# Write transform results into parquet format

transformed_data.write.mode("overwrite").parquet("/mnt/processed_data")

#  list of files in processed mount location

dbutils.fs.ls("/mnt/processed_data")
# MAGIC %pip install snowflake-connector-python
# MAGIC %pip install snowflake-sqlalchemy
# MAGIC

# COMMAND ----------

snowflake_options = {
    "sfURL": "https://jaqhoasasnw-wt95891.snowflakecomputing.com",
    "sfDatabase": "Transformed_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN",
    "sfUser": "ACCENTFUTURE",
    "sfPassword": "************"
}

# COMMAND ----------

transformed_data.write \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "processed_data_table") \
    .mode("overwrite") \
    .save()

# COMMAND ----------

transformed_data.write.format("delta").mode("overwrite").save("/mnt/delta/processed_data")

# COMMAND ----------

dbutils.fs.ls("/mnt/delta/processed_data")