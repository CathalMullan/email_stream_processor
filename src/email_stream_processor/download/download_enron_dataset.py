"""
Download and extract the Enron dataset.
"""
import tarfile
from pathlib import Path
from typing import Optional

import requests
from tqdm import tqdm

from email_stream_processor.helpers.globals.directories import ENRON_DIR, list_files_in_folder

DOWNLOAD_LINK = "http://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz"

ENRON_ZIP_FILE = ENRON_DIR + "/enron_dataset.tar.gz"
ENRON_ZIP_FILE_SIZE = 443254787
ENRON_FILE_COUNT = 517403


def download_tarfile() -> None:
    """
    Download the tarfile.

    :return: None
    """
    with open(ENRON_ZIP_FILE, "ab") as file:
        pos = file.tell()

        if pos == ENRON_ZIP_FILE_SIZE:
            print("Zip file already downloaded.")
            return

        print("Beginning download...")
        resume_header = {"Accept-Encoding": "identity"}
        if pos:
            print(f"Continuing download from {pos} bytes...")
            resume_header["Range"] = f"bytes={pos}-"

        response = requests.get(DOWNLOAD_LINK, headers=resume_header, stream=True)
        content_length: Optional[str] = response.headers.get("Content-length")

        if content_length:
            total_length: int = int(content_length) // 1024

        for data in tqdm(iterable=response.iter_content(chunk_size=1024), total=total_length, unit="KB"):
            file.write(data)

    print("Finished downloading the dataset.")


def extract_tarfile() -> None:
    """
    Extract the tarfile.

    :return: None
    """
    size = len(list_files_in_folder(ENRON_DIR))

    if size == ENRON_FILE_COUNT:
        print("All files already extracted.")
        return

    print("Extracting the dataset...")
    with tarfile.open(ENRON_ZIP_FILE) as tar:
        all_members = tar.getmembers()
        for member in tqdm(iterable=all_members, total=len(all_members), unit="files"):
            tar.extract(member, ENRON_DIR)

    print("Finished extracting dataset.")


def main() -> None:
    """
    Download and extract the Enron dataset.

    :return: None
    """
    # Ensure path exists
    Path(ENRON_DIR).mkdir(parents=True, exist_ok=True)

    download_tarfile()
    extract_tarfile()
