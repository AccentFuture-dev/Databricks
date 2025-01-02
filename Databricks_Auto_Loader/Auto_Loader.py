from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# Define paths
source_path = "/mnt/source-data/"
checkpoint_path = "/mnt/checkpoint-data/"  # Update or clean up this directory as needed
target_path = "/mnt/transformed-data/"

# Define schema explicitly (example for transaction data)
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("product_category", StringType(), True),
])

# Read data using Auto Loader
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")  # Replace with the format of your data (e.g., "parquet", "csv")
    .option("cloudFiles.schemaLocation", checkpoint_path)  # For schema tracking
    .schema(schema)  # Explicit schema definition
    .load(source_path)
)

# Add a transformation (e.g., adding ingestion timestamp)
transformed_df = df.withColumn("ingestion_time", current_timestamp())

# Write data to the output directory in Delta format with append mode
write_query = (
    transformed_df.writeStream
    .format("delta")  # Use "parquet", "json", or "csv" if preferred
    .outputMode("append")  # Append new data
    .option("checkpointLocation", checkpoint_path)  # Checkpoint for fault tolerance
    .option("path", target_path)  # Target directory for output
    .start()
)

# Wait for the streaming query to terminate (optional for continuous jobs)
write_query.awaitTermination()