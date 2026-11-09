
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dp.table(
    name="city_bank.gold.transaction_count_by_minute_sliding_window",
    comment="Alert details where transactions have exceeded the customer's configured transaction limit"
)
def transaction_count_by_minute() -> DataFrame:

    # Read Silver transactions as streaming data
    transaction = spark.readStream.table(
        "city_bank.silver.transactions"
    )

    transaction_with_watermark=transaction.withWatermark("silver_ingestion_timestamp", "5 minutes")
    transaction_count_df=transaction_with_watermark.groupBy(F.window("silver_ingestion_timestamp", "5 minute","1 minute")).agg(F.count("*").alias("transaction_count")).select(F.col("window.start").alias("window_start"),F.col("window.end").alias("window_end"),F.col("transaction_count"))
    return transaction_count_df
