"""
Spark Structured Streaming cluster parsing raw emails from Kafka queue and converting to TensorFlow parsable format.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StringType, StructField, StructType

from email_stream_processor.helpers.config.get_config import CONFIG

# Representation of Message Contents Dict in Spark
MESSAGE_CONTENTS_STRUCT = StructType(
    [
        StructField(name="message_id", dataType=StringType(), nullable=True),
        StructField(name="date", dataType=StringType(), nullable=True),
        StructField(name="from_address", dataType=StringType(), nullable=True),
        StructField(name="to_addresses", dataType=StringType(), nullable=True),
        StructField(name="bcc_addresses", dataType=StringType(), nullable=True),
        StructField(name="subject", dataType=StringType(), nullable=True),
        StructField(name="body", dataType=StringType(), nullable=True),
    ]
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
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4") \
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

    # Parse to strings
    data_frame = data_frame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # Updated Kafka schema
    data_frame.printSchema()
    # root
    #  |-- key: string (nullable = true)
    #  |-- value: string (nullable = true)

    def print_value(data_frame: DataFrame, epoch_id: int) -> None:
        """
        Print the value.

        :param data_frame: partitioned data frame micro batch
        :param epoch_id: unique id of micro batch
        :return: None
        """
        print(f"Processing: #{epoch_id}")
        print(f"Batch Size: #{data_frame.count()}")

    streaming_query: StreamingQuery = data_frame.writeStream.foreachBatch(print_value).start()

    streaming_query.awaitTermination()

    print("Done.")

    # def eml_bytes_to_spark_message_contents(eml_bytes: bytearray) -> Optional[Dict[str, str]]:
    #     """
    #     Process eml file as bytes and convert to message content dict.
    #
    #     :param eml_bytes: bytes representation of an eml file
    #     :return: optional message content
    #     """
    #     eml_str = eml_bytes.decode()
    #     logger.warn(f"Raw Email: {eml_str}")
    #
    #     email_message: Optional[EmailMessage] = read_message_from_string(eml_str)
    #     if not isinstance(email_message, EmailMessage):
    #         return None
    #
    #     message_contents: Optional[MessageContent] = extract_message_contents(email_message)
    #     if not isinstance(message_contents, MessageContent):
    #         return None
    #
    #     message_contents_dict: Dict[str, str] = message_contents.as_dict()
    #     return message_contents_dict
    #
    # # Attempt to parse eml string to a struct
    # udf_eml_bytes_to_spark_message_contents: UserDefinedFunction = udf(
    #     eml_bytes_to_spark_message_contents, returnType=MESSAGE_CONTENTS_STRUCT
    # )
    #
    # email_data_frame: DataFrame = data_frame.select(
    #     udf_eml_bytes_to_spark_message_contents("value").alias("email_struct")
    # )
    #
    # # Email contents schema
    # email_data_frame.printSchema()
    # # root
    # #  |-- email_struct: struct (nullable = true)
    # #  |    |-- message_id: string (nullable = true)
    # #  |    |-- date: string (nullable = true)
    # #  |    |-- from_address: string (nullable = true)
    # #  |    |-- to_addresses: string (nullable = true)
    # #  |    |-- bcc_addresses: string (nullable = true)
    # #  |    |-- subject: string (nullable = true)
    # #  |    |-- body: string (nullable = true)
    #
    # # Grab just the body
    # body_data_frame: DataFrame = email_data_frame.select("email_struct.body")
    #
    # Print the processed body as they events are received.
    # # fmt: off
    # streaming_query: StreamingQuery = body_data_frame.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .start()
    # # fmt: on
    #
    # streaming_query.awaitTermination()


if __name__ == "__main__":
    main()
