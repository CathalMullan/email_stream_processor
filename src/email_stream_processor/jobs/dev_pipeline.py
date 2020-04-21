"""
Complete pipeline to be run locally using the Enron dataset.
"""
import pickle  # nosec
from multiprocessing import Pool
from pathlib import Path
from typing import List, Optional

import numpy as np
from pyspark.sql import DataFrame
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import CountVectorizer
from tqdm import tqdm

from email_stream_processor.helpers.globals.directories import (
    ENRON_DIR,
    PARQUET_DIR,
    PROCESSED_DIR,
    list_files_in_folder,
)
from email_stream_processor.helpers.input.input_parquet import read_data_frame_from_parquet
from email_stream_processor.helpers.output.output_parquet import output_parquet
from email_stream_processor.jobs.topic_model_preprocessing import DICTIONARY, text_lemmatize_and_lower
from email_stream_processor.parsing.message_contents_extraction import MessageContent, eml_path_to_message_contents


def join_array_map(text: List[str]) -> str:
    """
    Combine list of strings into a string.

    :param text: list of strings
    :return: concatenated string
    """
    return " ".join(map(str, text))


def dev_topic_model(message_contents: DataFrame) -> None:
    """
    Run entire pipeline locally.

    :param message_contents: dataframe containing email files
    :return: None
    """
    data_frame_length = len(message_contents)
    print(f"Processing {data_frame_length} records.")

    print("Lemmatizing text.")

    with Pool(processes=12) as pool:
        message_contents["clean_subject"] = list(
            tqdm(pool.imap(text_lemmatize_and_lower, message_contents["subject"]), total=data_frame_length)
        )

        message_contents["clean_body"] = list(
            tqdm(pool.imap(text_lemmatize_and_lower, message_contents["body"]), total=data_frame_length)
        )

        message_contents["processed_subject"] = list(
            tqdm(pool.imap(join_array_map, message_contents["clean_subject"]), total=data_frame_length)
        )

        message_contents["processed_body"] = list(
            tqdm(pool.imap(join_array_map, message_contents["clean_body"]), total=data_frame_length)
        )

    vectorizer = CountVectorizer(
        decode_error="replace", strip_accents="unicode", lowercase="true", vocabulary=DICTIONARY
    )

    print("Vectorizing text.")
    subject_document = vectorizer.fit_transform(message_contents["processed_subject"])
    body_document = vectorizer.fit_transform(message_contents["processed_body"])

    set_size: int = data_frame_length // 2

    print("Exporting 'train' subject data.")
    train_subject_data: csr_matrix = subject_document[:set_size, :].astype(np.float32)
    with open(f"{PROCESSED_DIR}/train_subject.npz", "wb") as file:
        pickle.dump(train_subject_data, file=file)

    print("Exporting 'train' body data.")
    train_body_data: csr_matrix = body_document[:set_size, :].astype(np.float32)
    with open(f"{PROCESSED_DIR}/train_body.npz", "wb") as file:
        pickle.dump(train_body_data, file=file)

    print("Exporting 'test' subject data.")
    test_subject_data: csr_matrix = subject_document[set_size:, :].astype(np.float32)
    with open(f"{PROCESSED_DIR}/test_subject.npz", "wb") as file:
        pickle.dump(test_subject_data, file=file)

    print("Exporting 'test' body data.")
    test_body_data: csr_matrix = body_document[set_size:, :].astype(np.float32)
    with open(f"{PROCESSED_DIR}/test_body.npz", "wb") as file:
        pickle.dump(test_body_data, file=file)


def main() -> None:
    """
    Read in the Enron dataset, parse out contents while anonymizing them then save to an parquet file and eml files.

    :return: None
    """
    # stream_pipeline.py
    file_paths: List[Path] = list_files_in_folder(f"{ENRON_DIR}/maildir")
    with Pool(processes=12) as pool:
        optional_message_contents: List[Optional[MessageContent]] = list(
            tqdm(pool.imap(eml_path_to_message_contents, file_paths), total=len(file_paths))
        )

    message_contents: List[MessageContent] = [message for message in optional_message_contents if message]
    output_parquet(message_contents, file_name="processed_enron")

    data_frame_message_contents: Optional[DataFrame] = read_data_frame_from_parquet(
        f"{PARQUET_DIR}/processed_enron.parquet.snappy"
    )

    # topic_model_preprocessing.py
    dev_topic_model(data_frame_message_contents)


if __name__ == "__main__":
    main()
