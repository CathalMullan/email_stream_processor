"""
End to end processing on the Enron dataset.
"""
import time
from multiprocessing.pool import Pool
from pathlib import Path
from typing import List, Optional

from email_stream_processor.helpers.globals.directories import ENRON_DIR, list_files_in_folder
from email_stream_processor.helpers.output.output_parquet import output_parquet
from email_stream_processor.parsing.message_contents_extraction import MessageContent, eml_path_to_message_contents


def main() -> None:
    """
    Read in the Enron dataset, parse out contents while anonymizing them then save to a parquet file and eml files.

    :return: None
    """
    file_paths: List[Path] = list_files_in_folder(f"{ENRON_DIR}/maildir")
    start_time: int = int(time.time())

    with Pool(processes=24) as pool:
        optional_message_contents: List[Optional[MessageContent]] = pool.map(eml_path_to_message_contents, file_paths)

    message_contents: List[MessageContent] = [message for message in optional_message_contents if message]
    output_parquet(message_contents, file_name="processed_enron_50000")

    print(f"Count: {len(message_contents)}")
    print(f"Finish: {int(time.time()) - start_time} seconds", flush=True)


if __name__ == "__main__":
    main()
