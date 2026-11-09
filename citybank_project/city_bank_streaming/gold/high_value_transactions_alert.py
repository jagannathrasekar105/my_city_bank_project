
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dp.table(
    name="city_bank.gold.high_value_transactions_alert",
    comment="Alert details where transactions have exceeded the customer's configured transaction limit"
)
def high_value_transactions_alert() -> DataFrame:

    # Read Silver transactions as streaming data
    transaction = spark.readStream.table(
        "city_bank.silver.transactions"
    )

    # Read Silver customers as static data
    customers = spark.read.table(
        "city_bank.silver.customers"
    )

    # Join transactions with customer details
    join_df = (
        transaction.join(
            customers,
            transaction.customer_id == customers.customer_id,
            "left"
        )
        # Filter high-value transactions
        .filter(
            F.col("amount") > F.col("transaction_limit")
        )
        # Select alert details
        .select(
            F.concat_ws(
                "_",
                F.lit("ALERT"),
                F.col("transaction_id")
            ).alias("alert_id"),

            F.lit("HIGH_VALUE_TRANSACTION").alias("alert_type"),

            F.current_timestamp().alias("alert_timestamp"),

            transaction.transaction_id,
            transaction.customer_id,

            customers.email.alias("customer_email"),

            F.concat_ws(
                " ",
                customers.first_name,
                customers.last_name
            ).alias("customer_name"),

            transaction.amount.alias("transaction_amount"),

            customers.transaction_limit,

            transaction.currency,
            transaction.merchant_name,
            transaction.merchant_category,
            transaction.transaction_type,
            transaction.payment_channel,
            transaction.city,
            transaction.country,
            transaction.is_international,
            transaction.transaction_timestamp,
            transaction.status
        )
    )

    return join_df
