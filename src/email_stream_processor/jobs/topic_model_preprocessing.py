"""
Read in Parquet file containing processed eml data, and vectorize to Numpy arrays.
"""
import os
import pickle  # nosec
import string
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import gcsfs
import numpy as np
import pandas
import spacy
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import monotonically_increasing_id, udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.udf import UserDefinedFunction
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import CountVectorizer
from spacy.tokens.token import Token

from email_stream_processor.helpers.config.get_config import CONFIG
from email_stream_processor.jobs.stream_pipeline import SPARK_JARS

# https://blog.dominodatalab.com/making-pyspark-work-spacy-overcoming-serialization-errors/
# spaCy isn't serializable but loading it is semi-expensive
SPACY = spacy.load("en_core_web_sm")
FILESYSTEM = gcsfs.GCSFileSystem()


def build_dictionary() -> Dict[str, int]:
    """
    Read static dictionary text file and create vocabulary of terms.

    :return:
    """
    dictionary_set = sorted(set(line.strip().lower() for line in open(CONFIG.dictionary_path)))
    dictionary_dict = {value: key for key, value in enumerate(dictionary_set)}
    return dictionary_dict


DICTIONARY = build_dictionary()


def is_valid_token(token: Token) -> bool:
    """
    Verify a token is fix for our topic detection purposes.

    :param token: a spaCy token
    :return: bool if valid
    """
    if token.like_num or token.like_email or token.like_url:
        return False

    if len(token.pos_) <= 2 or len(token.lemma_) <= 2:
        return False

    if token.lemma_.lower() not in DICTIONARY.keys():
        return False

    return True


def text_lemmatize_and_lower(text: Optional[str]) -> List[str]:
    """
    Remove unwanted characters from text, using spaCy and it's part of speech tagging.

    Strip punctuation and stop words.
    Convert words to their root form.

    :param text: dirty text to be lemmatized
    :return: text cleaned of unwanted characters, lemmatized and lowered
    """
    if not text:
        return [""]

    # Remove punctuation using C lookup table
    # https://stackoverflow.com/a/266162
    text = text.translate(str.maketrans("", "", string.punctuation))
    text_doc = SPACY(text)

    clean_tokens: List[str] = []
    for token in text_doc:
        if is_valid_token(token):
            clean_tokens.append(token.lemma_.lower())

    return clean_tokens


def tidy_dataset(dataset: pandas.DataFrame) -> pandas.DataFrame:
    """
    Remove empty rows.

    :param dataset: pandas data frame containing 'subject' and 'body'.
    :return: dataset without empty strings/nulls.
    """
    dataset.replace("", np.nan, inplace=True)
    dataset.dropna(inplace=True)
    return dataset


def main() -> None:
    """
    Vectorize Email Parquet to Numpy arrays.

    JVM forking error?
    sudo hostname -s 127.0.0.1

    :return: None
    """
    os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"

    # fmt: off
    if CONFIG.is_dev:
        spark = SparkSession.builder \
            .master("local[4]") \
            .appName("topic_model_preprocessing") \
            .config("spark.jars", SPARK_JARS) \
            .config("spark.kubernetes.authenticate.driver.serviceAccountName", "streaming") \
            .config("spark.kubernetes.container.image", "topic_model_preprocessing") \
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
            .appName("topic_model_preprocessing") \
            .config("spark.jars", SPARK_JARS) \
            .getOrCreate()
    # fmt: on

    # Access the JVM logging context.
    # noinspection All
    jvm_logger = spark.sparkContext._jvm.org.apache.log4j
    logger = jvm_logger.LogManager.getLogger(__name__)
    logger.info("Beginning Topic Modelling Processing Job.")

    # Process previous days' data.
    yesterdays_date = datetime.strftime(datetime.now() - timedelta(1), "%Y-%m-%d")
    data_source = f"{CONFIG.bucket_parquet}/processed_date={yesterdays_date}/"

    # fmt: off
    print("Loading dataset.")
    data_frame: DataFrame = spark.read \
        .format("parquet") \
        .option("compression", "snappy") \
        .load(data_source) \
        .select("subject", "body") \
        .withColumn("id", monotonically_increasing_id()) \
        .repartition(4)
    print("Dataset loaded.")

    udf_text_lemmatize_and_lower: UserDefinedFunction = udf(text_lemmatize_and_lower, ArrayType(StringType()))

    print("Lemmatizing data.")
    data_frame = data_frame \
        .withColumn("processed_subject", udf_text_lemmatize_and_lower("subject")) \
        .withColumn("processed_body", udf_text_lemmatize_and_lower("body"))
    print("Finished lemmatizing data.")
    # fmt: on

    print("Converting to Pandas.")
    pd_data_frame = data_frame.select("id", "processed_subject", "processed_body").toPandas()

    print("Flattening data.")
    pd_data_frame["processed_subject"] = [" ".join(map(str, line)) for line in pd_data_frame["processed_subject"]]
    pd_data_frame["processed_body"] = [" ".join(map(str, line)) for line in pd_data_frame["processed_body"]]

    vectorizer = CountVectorizer(
        decode_error="replace", strip_accents="unicode", lowercase="true", vocabulary=DICTIONARY
    )

    print("Vectorizing data.")
    subject_document = vectorizer.fit_transform(pd_data_frame["processed_subject"])
    body_document = vectorizer.fit_transform(pd_data_frame["processed_body"])
    print("Finished vectorizing data.")

    set_size = data_frame.count() // 2
    bucket_output = f"{CONFIG.bucket_parquet}matrix_date={yesterdays_date}/"

    print("Exporting 'train' subject data.")
    train_subject_data: csr_matrix = subject_document[:set_size, :].astype(np.float32)
    with FILESYSTEM.open(f"{bucket_output}train_subject.npz", "wb") as file:
        pickle.dump(train_subject_data, file=file)

    print("Exporting 'train' body data.")
    train_body_data: csr_matrix = body_document[:set_size, :].astype(np.float32)
    with FILESYSTEM.open(f"{bucket_output}train_body.npz", "wb") as file:
        pickle.dump(train_body_data, file=file)

    print("Exporting 'test' subject data.")
    test_subject_data: csr_matrix = subject_document[set_size:, :].astype(np.float32)
    with FILESYSTEM.open(f"{bucket_output}test_subject.npz", "wb") as file:
        pickle.dump(test_subject_data, file=file)

    print("Exporting 'test' body data.")
    test_body_data: csr_matrix = body_document[set_size:, :].astype(np.float32)
    with FILESYSTEM.open(f"{bucket_output}test_body.npz", "wb") as file:
        pickle.dump(test_body_data, file=file)

    print("Completed vectorization.")


if __name__ == "__main__":
    main()
