from pyspark import pipelines as dp
from pyspark.sql import DataFrame
import json
from pyspark.sql import functions as F

@dp.table(
    name="city_bank.bronze.transactions",
    comment="Transaction raw stream data ingested from kafka"
)
def transaction_brnze() -> DataFrame:
    kafka_connection_json=dbutils.secrets.get(scope="city_bank-scope",key="kafka_connection_details")
    kafka_config=json.loads(kafka_connection_json)
    api_key=kafka_config['api_key']
    api_secret=kafka_config['api_secret']
    bootstrap_server=kafka_config['bootstrap_servers']
    topic=kafka_config['topic']

    jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'

    stream_df=(spark.readStream.format('kafka')
              .option('kafka.bootstrap.servers',bootstrap_server)
              .option('subscribe',topic)
              .option('kafka.sasl.mechanism','PLAIN')
              .option('kafka.security.protocol','SASL_SSL')
              .option('kafka.sasl.jaas.config',jaas_config)
              .option('startingoffsets','earliest')
              .load()
             
              )
    parsed_streaming_df=stream_df.select(
    F.col("key").cast("string"),
    F.col("value").cast("string"),
    F.col("topic"),
    F.col("partition"),
    F.col("offset"),
    F.col("timestamp"),
    F.col("timestampType"),
    F.current_timestamp().alias("ingestion_timestamp")
    )

    return parsed_streaming_df

