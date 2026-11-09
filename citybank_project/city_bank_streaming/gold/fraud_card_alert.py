
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dp.table(
    name="city_bank.gold.fraud_card_alert",
    comment="Alert details where transactions have exceeded the customer's configured transaction limit"
)
def fraud_card_alert() -> DataFrame:

    # Read Silver transactions as streaming data
    transaction = spark.readStream.table(
        "city_bank.silver.transactions"
    )

    fraud_watchlist=spark.readStream.table(
        "city_bank.silver.fraud_watchlist"
    )
    customers = spark.read.table(
        "city_bank.silver.customers"
    )

    transaction_with_watermark=transaction.withWatermark("silver_ingestion_timestamp", "5 minutes")
    fraud_watchlist_with_watermark=fraud_watchlist.withWatermark("effective_from", "5 minutes")

    fraud_detected=(
        transaction_with_watermark.join(fraud_watchlist_with_watermark,transaction_with_watermark.card_number==fraud_watchlist_with_watermark.entity_id,"inner")
                    
        .join(customers,transaction_with_watermark.customer_id==customers.customer_id,"left")
            .select( 
                    F.concat_ws("_",F.lit("Fraud"),F.col("watchlist_id")).alias("alert_id"),
                  F.lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
                  F.current_timestamp().alias("alert_timestamp"),

                #   transaction details
                 transaction_with_watermark.transaction_id.alias("transaction_id"),
                 transaction_with_watermark.customer_id.alias("customer_id"),
                 customers.email.alias("email"),
                 F.concat_ws("_",customers.first_name,customers.last_name).alias("customer_name"),
                 transaction_with_watermark.amount.alias("amount"),
                 transaction_with_watermark.card_number.alias("card_number"),
                  transaction_with_watermark.merchant_id.alias("merchant_id"),
                  transaction_with_watermark.merchant_name.alias("merchant_name"),
                transaction_with_watermark.merchant_category.alias("merchant_category"),
                 transaction_with_watermark.transaction_type.alias("transaction_type"),
                 transaction_with_watermark.payment_channel.alias("payment_channel"),
                 transaction_with_watermark.device_id.alias("device_id"),
                 transaction_with_watermark.city.alias("city"),
                 transaction_with_watermark.country.alias("country"),
                 transaction_with_watermark.transaction_timestamp.alias("transaction_timestamp"),
                 transaction_with_watermark.is_international.alias("is_international"),
                 transaction_with_watermark.status.alias("status"),

                #  fraud_watchlist_details
                fraud_watchlist_with_watermark.watchlist_id.alias("watchlist_id"),
                 fraud_watchlist_with_watermark.risk_level.alias("risk_level"),
                  fraud_watchlist_with_watermark.watch_type.alias("watch_type"),
                 fraud_watchlist_with_watermark.action.alias("action"),
                 fraud_watchlist_with_watermark.reason_code.alias("reason_code"),
                 fraud_watchlist_with_watermark.reason_description.alias("reason_description"),
                 fraud_watchlist_with_watermark.effective_from.alias("watchlist_effective_from"),
                 fraud_watchlist_with_watermark.reported_by,
                 fraud_watchlist_with_watermark.reported_source,
                 fraud_watchlist_with_watermark.city.alias("watchlist_city"),
                 fraud_watchlist_with_watermark.country.alias("watchlist_country")                 
                 )
    )

    # Read Silver customers as static data
    

    return fraud_detected
