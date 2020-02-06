"""
Spark Structured Streaming cluster parsing raw emails from Kafka queue and converting to TensorFlow parsable format.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from email_stream_processor.helpers.config.get_config import CONFIG


def main() -> None:
    """
    Spark Structured Streaming cluster parsing raw emails from Kafka queue and converting to TensorFlow parsable format.

    :return: None
    """
    # fmt: off
    spark: SparkSession = SparkSession.builder \
        .master("local[4]") \
        .appName("stream_pipeline") \
        .getOrCreate()
    # fmt: on

    # Access the JVM logging context.
    jvm_logger = spark.sparkContext._jvm.org.apache.log4j
    logger = jvm_logger.LogManager.getLogger(__name__)
    logger.info("Beginning email stream processing pipeline.")

    # fmt: off
    data_frame: DataFrame = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", CONFIG.kafka_hosts) \
        .option("subscribe", "email") \
        .option("startingOffsets", "earliest") \
        .load()

    streaming_query: StreamingQuery = data_frame.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    # fmt: on

    streaming_query.awaitTermination()


if __name__ == "__main__":
    main()
