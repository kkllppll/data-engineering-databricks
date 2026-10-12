from pyspark import pipelines as dp
from pyspark.sql import functions as F

EVENTHUB_NAMESPACE = spark.conf.get("eventhub.namespace")
EVENTHUB_NAME = spark.conf.get("eventhub.name")
SECRET_SCOPE = spark.conf.get("eventhub.secret_scope")
SECRET_KEY = spark.conf.get("eventhub.secret_key")

KAFKA_PORT = 9093


def _kafka_options() -> dict:
    connection_string = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
    # Event Hubs' Kafka endpoint expects the literal user "$ConnectionString" and the connection string as password
    jaas_config = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{connection_string}";'
    )
    return {
        "kafka.bootstrap.servers": f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:{KAFKA_PORT}",
        "subscribe": EVENTHUB_NAME,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas_config,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
    }


@dp.table(
    name="wikipedia_recentchange",
    comment="Raw Wikipedia recent-change events consumed from Event Hub via its Kafka endpoint",
    table_properties={
        # Events cannot be re-read from Event Hub after retention, so a full refresh would lose data permanently
        "pipelines.reset.allowed": "false",
        "delta.appendOnly": "true",
    },
)
def wikipedia_recentchange():
    return (
        spark.readStream.format("kafka")
        .options(**_kafka_options())
        .load()
        .select(
            F.col("value").cast("string").alias("raw_payload"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("eventhub_enqueued_at"),
            F.current_timestamp().alias("ingestion_timestamp"),
        )
    )
