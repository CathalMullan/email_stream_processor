"""
Spark Structured Streaming cluster parsing raw emails from Kafka queue and converting to TensorFlow parsable format.
"""
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.udf import UserDefinedFunction

from email_stream_processor.helpers.config.get_config import CONFIG
from email_stream_processor.parsing.message_contents_extraction import (
    MESSAGE_CONTENTS_STRUCT,
    eml_str_to_spark_message_contents,
)


def main() -> None:
    """
    Spark Structured Streaming cluster parsing raw emails from Kafka queue and converting to TensorFlow parsable format.

    :return: None
    """
    os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"

    # fmt: off
    spark: SparkSession = SparkSession.builder.getOrCreate()

    # Access the JVM logging context.
    spark.sparkContext.setLogLevel("WARN")
    # noinspection All
    jvm_logger = spark.sparkContext._jvm.org.apache.log4j
    logger = jvm_logger.LogManager.getLogger(__name__)
    logger.warn("Beginning Kafka email stream processing pipeline.")

    data_frame: DataFrame = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", CONFIG.kafka_hosts) \
        .option("compression", "snappy") \
        .option("startingOffsets", "earliest") \
        .option("subscribe", CONFIG.kafka_topic) \
        .load()

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

    udf_eml_str_to_spark_message_contents: UserDefinedFunction = udf(
        f=eml_str_to_spark_message_contents, returnType=MESSAGE_CONTENTS_STRUCT
    )

    streaming_query: StreamingQuery = data_frame \
        .selectExpr("CAST(value AS STRING)") \
        .withColumn("processed_text", udf_eml_str_to_spark_message_contents(col("value"))) \
        .writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", CONFIG.bucket_parquet) \
        .option("checkpointLocation", CONFIG.bucket_checkpoint) \
        .trigger(processingTime="1 minute") \
        .start()

    streaming_query.awaitTermination()
    # fmt: on


if __name__ == "__main__":
    main()
