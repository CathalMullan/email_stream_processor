"""
Read a Parquet file into a Dataframe.
"""
from pathlib import Path
from typing import List, Optional, Union

import pyarrow.parquet
from pandas import DataFrame


def read_dataframe_from_parquet(parquet_file: Union[Path, str], columns: Optional[List[str]]) -> Optional[DataFrame]:
    """
    Read a Parquet file into a Dataframe.

    :param parquet_file: path to a Parquet file
    :param columns: specific columns to retrieve from parquet file
    :return: DataFrame of Parquet input file
    """
    if not isinstance(parquet_file, Path):
        parquet_file = Path(parquet_file)

    if not parquet_file.exists():
        return None

    if columns:
        data_frame = pyarrow.parquet.read_table(parquet_file, columns=columns).to_pandas()
    else:
        data_frame = pyarrow.parquet.read_table(parquet_file).to_pandas()

    return data_frame
