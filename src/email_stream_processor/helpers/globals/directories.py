"""
Directory traversal helper, and general path functions.
"""
from os.path import abspath, dirname
from pathlib import Path
from typing import List, Union

PROJECT_DIR = dirname(dirname(dirname(dirname(dirname(abspath(__file__))))))

CONFIG_DIR = PROJECT_DIR + "/config"
MODELS_DIR = PROJECT_DIR + "/models"
MAVEN_DIR = PROJECT_DIR + "/maven"

DATA_DIR = PROJECT_DIR + "/data"
RAW_DIR = DATA_DIR + "/raw"
ENRON_DIR = RAW_DIR + "/enron"

PROCESSED_DIR = DATA_DIR + "/processed"
PARQUET_DIR = PROCESSED_DIR + "/parquet"
CLEAN_ENRON_DIR = PROCESSED_DIR + "/clean_enron"

TESTS_DIR = PROJECT_DIR + "/tests"
TESTS_DATA_DIR = TESTS_DIR + "/data"
TESTS_EMAIL_DIR = TESTS_DATA_DIR + "/emails"
TESTS_PARQUET_DIR = TESTS_DATA_DIR + "/parquet"


def list_files_in_folder(folder_path: Union[Path, str]) -> List[Path]:
    """
    List all files in a folder recursively.

    :param folder_path: starting point of search
    :return: list of paths to files
    """
    if not isinstance(folder_path, Path):
        folder_path = Path(folder_path)

    all_paths = list(folder_path.rglob(f"**/*"))

    # Strip non-files
    file_paths: List[Path] = [path for path in all_paths if path.is_file()]

    return file_paths
