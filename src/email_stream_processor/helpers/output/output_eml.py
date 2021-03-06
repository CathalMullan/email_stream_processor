"""
Convert a list of message contents into eml files.
"""
import time
from pathlib import Path
from typing import List

from email_stream_processor.helpers.globals.directories import CLEAN_ENRON_DIR
from email_stream_processor.parsing.message_contents_extraction import MessageContent


def output_eml(message_contents: List[MessageContent]) -> None:
    """
    Convert a list of message contents into eml files and save to processed clean Enron directory.

    :param message_contents: list of parsed message contents
    :return:
    """
    Path(CLEAN_ENRON_DIR).mkdir(exist_ok=True, parents=True)

    for message_content in message_contents:
        generated_file_name = f"{CLEAN_ENRON_DIR}/{int(round(time.time() * 1000))}"
        with open(generated_file_name, "w") as file:
            file.write(message_content.as_str() + "\n")
            file.write("\n")
