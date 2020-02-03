"""
Spark Structured Streaming cluster parsing raw emails from Kafka queue and converting to TensorFlow parsable format.
"""
from pyspark.sql import SparkSession


def main() -> None:
    """
    Spark Structured Streaming cluster parsing raw emails from Kafka queue and converting to TensorFlow parsable format.

    :return: None
    """
    # fmt: off
    spark: SparkSession = SparkSession.builder \
        .master("local[4]") \
        .appName("topic_modelling") \
        .getOrCreate()
    # fmt: on

    # Access the JVM logging context.
    jvm_logger = spark.sparkContext._jvm.org.apache.log4j
    logger = jvm_logger.LogManager.getLogger(__name__)
    logger.info("Beginning email stream processing pipeline.")

    # fmt: off
    events = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "email") \
        .load()
    # fmt: on

    events.show(truncate=False)


if __name__ == "__main__":
    main()
