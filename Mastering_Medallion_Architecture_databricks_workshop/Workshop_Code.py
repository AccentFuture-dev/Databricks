#Workshop Code


configs = {
    "fs.azure.account.key.developmentrawdata.blob.core.windows.net": "SDQUMP+Wx7k6VFVL6vLp+dVRIjGLSUjEXHjg=="
}
dbutils.fs.mount(
    source="wasbs://bronze-container@developmentrawdata.blob.core.windows.net/",
    mount_point="/mnt/bronze",
    extra_configs=configs
)

dbutils.fs.mount(
    source="wasbs://silver-container@developmentrawdata.blob.core.windows.net/",
    mount_point="/mnt/silver",
    extra_configs=configs
)


dbutils.fs.mount(
    source="wasbs://gold-container@developmentrawdata.blob.core.windows.net/",
    mount_point="/mnt/gold",
    extra_configs=configs
)


dbutils.fs.ls("/mnt/")

# Load raw data from Bronze
bronze_path = "/mnt/bronze/sample_1k_data.csv"

bronze_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(bronze_path)

# Write to Delta Lake
bronze_table_path = "/mnt/bronze/delta/"
bronze_df.write.format("delta").mode("overwrite").save(bronze_table_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS bronze_table USING DELTA LOCATION '{bronze_table_path}'")

from pyspark.sql.functions import to_timestamp, col

# Load data from Bronze layer
bronze_df = spark.read.format("delta").load(bronze_table_path)

# Data cleansing and transformation
silver_df = bronze_df.filter(col("event").isNotNull()) \
    .withColumn("event_time", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")) \
    .drop("timestamp")

# Write to Delta Lake
silver_table_path = "/mnt/silver/delta/"
silver_df.write.format("delta").mode("overwrite").save(silver_table_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS silver_table USING DELTA LOCATION '{silver_table_path}'")


from pyspark.sql.functions import count

# Load data from Silver layer
silver_df = spark.read.format("delta").load(silver_table_path)

# Aggregate data
gold_df = silver_df.groupBy("event").agg(count("user_id").alias("event_count"))

# Write to Delta Lake
gold_table_path = "/mnt/gold/delta/"
gold_df.write.format("delta").mode("overwrite").save(gold_table_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS gold_table USING DELTA LOCATION '{gold_table_path}'")


%sql
SELECT event, event_count
FROM gold_table
ORDER BY event_count DESC;
