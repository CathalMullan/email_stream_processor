"""
Spark Structured Streaming cluster parsing raw emails from Kafka queue and converting to TensorFlow parsable format.
"""
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date, udf
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.udf import UserDefinedFunction

from email_stream_processor.helpers.config.get_config import CONFIG
from email_stream_processor.parsing.message_contents_extraction import (
    MESSAGE_CONTENTS_STRUCT,
    eml_str_to_spark_message_contents,
)

SPARK_JARS = "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/2.4.5/spark-sql-kafka-0-10_2.11-2.4.5.jar,https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.4.1/kafka-clients-2.4.1.jar,https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop2-2.0.1/gcs-connector-hadoop2-2.0.1-shaded.jar"  # noqa # pylint: disable=line-too-long


def main() -> None:
    """
    Spark Structured Streaming cluster parsing raw emails from Kafka queue and converting to TensorFlow parsable format.

    JVM forking error?
    sudo hostname -s 127.0.0.1

    :return: None
    """
    os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"

    # fmt: off
    if CONFIG.is_dev:
        spark = SparkSession.builder \
            .master("local[4]") \
            .appName("email_stream_processor") \
            .config("spark.jars", SPARK_JARS) \
            .config("spark.kubernetes.authenticate.driver.serviceAccountName", "streaming") \
            .config("spark.kubernetes.container.image", "email_stream_processor") \
            .config("spark.kubernetes.namespace", "streaming") \
            .config("spark.kubernetes.driver.secrets.service-account", "/etc/secrets") \
            .config("spark.kubernetes.executor.secrets.service-account", "/etc/secrets") \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
            .config("spark.hadoop.fs.gs.project.id", "distributed-email-pipeline") \
            .config("spark.hadoop.fs.gs.auth.service.account.enable", "true") \
            .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", CONFIG.gcp_credentials) \
            .getOrCreate()
    else:
        spark = SparkSession.builder \
            .appName("email_stream_processor") \
            .getOrCreate()
    # fmt: on

    # Access the JVM logging context.
    # noinspection All
    jvm_logger = spark.sparkContext._jvm.org.apache.log4j
    logger = jvm_logger.LogManager.getLogger(__name__)
    logger.warn("Beginning Kafka email stream processing pipeline.")

    # fmt: off
    data_frame: DataFrame = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", CONFIG.kafka_hosts) \
        .option("compression", "snappy") \
        .option("failOnDataLoss", "false") \
        .option("startingOffsets", "earliest") \
        .option("subscribe", CONFIG.kafka_topic) \
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

    udf_eml_str_to_spark_message_contents: UserDefinedFunction = udf(
        f=eml_str_to_spark_message_contents, returnType=MESSAGE_CONTENTS_STRUCT
    )

    # fmt: off
    streaming_query: StreamingQuery = data_frame \
        .selectExpr("CAST(value AS STRING)", "timestamp") \
        .withColumn("message", udf_eml_str_to_spark_message_contents(col("value"))) \
        .withColumn("date", to_date(col("timestamp"), "yyyy-MM-dd")) \
        .select("message.*", "date") \
        .writeStream \
        .format("parquet") \
        .partitionBy("date") \
        .outputMode("append") \
        .option("path", CONFIG.bucket_parquet) \
        .option("checkpointLocation", CONFIG.bucket_checkpoint) \
        .trigger(processingTime="6 minute") \
        .start()
    # fmt: on

    streaming_query.awaitTermination()


if __name__ == "__main__":
    main()
