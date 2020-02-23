"""
Read in a basic Parquet file and verify it parses to Pandas.
"""
from typing import Optional

from pandas import DataFrame

from email_stream_processor.helpers.globals.directories import TESTS_PARQUET_DIR
from email_stream_processor.helpers.input.input_parquet import read_data_frame_from_parquet


def test_read_dataframe_from_parquet() -> None:
    """
    Read in a basic Parquet file and verify it parses to Pandas.

    :return: None
    """
    dataframe: Optional[DataFrame] = read_data_frame_from_parquet(
        parquet_file=TESTS_PARQUET_DIR + "/test.parquet.snappy", columns=None
    )
    assert isinstance(dataframe, DataFrame)

    dataframe_columns: Optional[DataFrame] = read_data_frame_from_parquet(
        parquet_file=TESTS_PARQUET_DIR + "/test.parquet.snappy", columns=["message_id"]
    )
    assert isinstance(dataframe_columns, DataFrame)

    no_dataframe: Optional[DataFrame] = read_data_frame_from_parquet(
        parquet_file=TESTS_PARQUET_DIR + "/random.parquet.snappy", columns=None
    )
    assert no_dataframe is None
