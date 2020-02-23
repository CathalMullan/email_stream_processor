"""
Read in Parquet file containing processed eml data, and vectorize to Numpy arrays.
"""
import pickle  # nosec
import string
from typing import List

import numpy as np
import spacy
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import monotonically_increasing_id, udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.udf import UserDefinedFunction
from scipy.sparse import save_npz
from sklearn.feature_extraction.text import CountVectorizer
from spacy.tokens.token import Token

from email_stream_processor.helpers.globals.directories import PARQUET_DIR, PROCESSED_DIR

# https://blog.dominodatalab.com/making-pyspark-work-spacy-overcoming-serialization-errors/
# spaCy isn't serializable but loading it is semi-expensive
SPACY = spacy.load("en_core_web_sm")


def is_valid_token(token: Token) -> bool:
    """
    Verify a token is fix for our topic detection purposes.

    :param token: a spaCy token
    :return: bool if valid
    """
    # See https://spacy.io/api/annotation#pos-universal
    if token.pos_ not in ["ADJ", "NOUN", "VERB"]:
        return False

    # How does oov play with custom vocab
    if token.is_stop or token.is_oov:
        return False

    if token.like_num or token.like_email or token.like_url:
        return False

    if len(token.pos_) <= 2 or len(token.lemma_) <= 2:
        return False

    # Enron specific problem - images have been previously mangled and replaced with [IMAGE] tag.
    if token.lemma_ and "image" in token.lemma_.lower():
        return False

    return True


def text_lemmatize_and_lower(text: str) -> List[str]:
    """
    Remove unwanted characters from text, using spaCy and it's part of speech tagging.

    Strip punctuation and stop words.
    Convert words to their root form.

    :param text: dirty text to be lemmatized
    :return: text cleaned of unwanted characters, lemmatized and lowered
    """
    # Remove punctuation using C lookup table
    # https://stackoverflow.com/a/266162
    text = text.translate(str.maketrans("", "", string.punctuation))
    text_doc = SPACY(text)

    clean_tokens: List[str] = []
    for token in text_doc:
        if is_valid_token(token):
            clean_tokens.append(token.lemma_.lower())

    return clean_tokens


def main() -> None:
    """
    Read in Parquet file containing processed eml data, and vectorize to Numpy arrays.

    JVM forking error?
        sudo hostname -s 127.0.0.1
        export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

    :return: None
    """
    # fmt: off
    spark: SparkSession = SparkSession.builder \
        .master("local[4]") \
        .appName("topic_modelling") \
        .config("spark.executor.memory", "6g") \
        .getOrCreate()
    # fmt: on

    # Access the JVM logging context.
    # noinspection All
    jvm_logger = spark.sparkContext._jvm.org.apache.log4j
    logger = jvm_logger.LogManager.getLogger(__name__)
    logger.info("Beginning Topic Modelling Processing Job.")

    # fmt: off
    data_frame: DataFrame = spark.read \
        .format("parquet") \
        .option("compression", "snappy") \
        .load(PARQUET_DIR + "/processed_enron_50000.parquet.snappy") \
        .select("body") \
        .withColumn("id", monotonically_increasing_id()) \
        .repartition(16)
    # fmt: on

    udf_text_lemmatize_and_lower: UserDefinedFunction = udf(text_lemmatize_and_lower, ArrayType(StringType()))
    data_frame = data_frame.withColumn("processed_text", udf_text_lemmatize_and_lower("body"))

    pd_data_frame = data_frame.select("processed_text").toPandas()
    pd_data_frame["processed_text"] = [",".join(map(str, line)) for line in pd_data_frame["processed_text"]]

    vectorizer = CountVectorizer(stop_words="english", max_df=0.9)
    term_document = vectorizer.fit_transform(pd_data_frame["processed_text"])

    set_size = data_frame.count() // 2
    save_npz(file=f"{PROCESSED_DIR}/train_data_50000.npz", matrix=term_document[:set_size, :].astype(np.float32))
    save_npz(file=f"{PROCESSED_DIR}/test_data_50000.npz", matrix=term_document[set_size:, :].astype(np.float32))
    with open(f"{PROCESSED_DIR}/dictionary_50000.pkl", "wb") as file:
        pickle.dump(vectorizer.vocabulary_, file)

    logger.info(f"Dictionary: {vectorizer.vocabulary_}")
    logger.info("Finished Topic Modelling Processing Job.")

    spark.stop()


if __name__ == "__main__":
    main()
