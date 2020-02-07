"""
Spark Structured Streaming cluster parsing raw emails from Kafka queue and converting to TensorFlow parsable format.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.udf import UserDefinedFunction

from email_stream_processor.helpers.config.get_config import CONFIG
from email_stream_processor.parsing.message_contents_extraction import (
    MESSAGE_CONTENTS_STRUCT,
    eml_bytes_to_spark_message_contents,
)


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

    # Set the logging output to WARN level.
    spark.sparkContext.setLogLevel("WARN")

    # Access the JVM logging context.
    jvm_logger = spark.sparkContext._jvm.org.apache.log4j
    logger = jvm_logger.LogManager.getLogger(__name__)
    logger.warn("Beginning Kafka email stream processing pipeline.")

    # fmt: off
    data_frame: DataFrame = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", CONFIG.kafka_hosts) \
        .option("compression", "snappy") \
        .option("startingOffsets", "earliest") \
        .option("subscribe", "email") \
        .load()
    # fmt: on

    # Raw Kafka schema
    data_frame.printSchema()
    # root
    #  |-- key: binary (nullable = true)
    #  |-- value: binary (nullable = true)
    #  |-- topic: string (nullable = true)
    #  |-- partition: integer (nullable = true)
    #  |-- offset: long (nullable = true)
    #  |-- timestamp: timestamp (nullable = true)
    #  |-- timestampType: integer (nullable = true)

    # Attempt to parse eml string to a struct
    udf_eml_bytes_to_spark_message_contents: UserDefinedFunction = udf(
        eml_bytes_to_spark_message_contents, returnType=MESSAGE_CONTENTS_STRUCT
    )

    email_data_frame: DataFrame = data_frame.select(
        udf_eml_bytes_to_spark_message_contents("value").alias("email_struct")
    )

    # Email contents schema
    email_data_frame.printSchema()
    # root
    #  |-- email_struct: struct (nullable = true)
    #  |    |-- message_id: string (nullable = true)
    #  |    |-- date: string (nullable = true)
    #  |    |-- from_address: string (nullable = true)
    #  |    |-- to_addresses: string (nullable = true)
    #  |    |-- bcc_addresses: string (nullable = true)
    #  |    |-- subject: string (nullable = true)
    #  |    |-- body: string (nullable = true)

    # Grab just the body
    body_data_frame: DataFrame = email_data_frame.select("email_struct.body")

    # Print the processed body as they events are received.
    # fmt: off
    streaming_query: StreamingQuery = body_data_frame.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    # fmt: on

    streaming_query.awaitTermination()


if __name__ == "__main__":
    main()
