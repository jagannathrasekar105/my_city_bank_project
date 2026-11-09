from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name="city_bank.bronze.fraud_watchlist",
    comment="fraud watchlist  raw stream data ingested by autoloader"
)
def fraud_watchlist_brnze() -> DataFrame:

   parsed_streaming_df=spark.readStream.format("cloudFiles").option("cloudFiles.format","json").option("cloudFiles.inferColumnTypes","true").load("/Volumes/city_bank/source/fraud_watchlist/source_data/")

   transformed_df=parsed_streaming_df.select(
       F.col("watchlist_id"),
       F.col("watch_type"),
       F.col("entity_id"),
       F.col("risk_level"),
       F.col("status"),
       F.col("action"),
       F.col("reported_by"),
       F.col("reported_source"),
       F.col("reason_code"),
       F.col("reason_description"),
       F.col("country"),
       F.col("city"),
       F.col("effective_from").alias("effective_from"),
       F.col("_rescued_data"),
       F.col("_metadata.file_path").alias("source_file"),
       F.current_timestamp().alias("ingestion_timestamp"))  

   return transformed_df

