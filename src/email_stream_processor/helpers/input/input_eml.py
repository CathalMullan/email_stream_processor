"""
Read all files in a directory recursively and parse to EmailMessages.
"""
from email import message_from_file, message_from_string
from email.errors import MessageDefect
from email.message import EmailMessage
from email.policy import strict
from pathlib import Path
from typing import List, Optional, Union

from email_stream_processor.helpers.globals.directories import list_files_in_folder


def read_message_from_string(message_str: str) -> Optional[EmailMessage]:
    """
    Parse a string to an EmailMessage.

    :param message_str: eml file as a string
    :return: parsed EmailMessage from string
    """
    try:
        # the policy 'strict' makes this return an EmailMessage class (Python 3.6+), rather than a Message class.
        # noinspection PyTypeChecker
        email_message: EmailMessage = message_from_string(message_str, policy=strict)  # type: ignore
    except MessageDefect:
        print(f"Could not parse string to EmailMessage")
        return None

    return email_message


def read_message_from_file(eml_path: Path) -> Optional[EmailMessage]:
    """
    Open an eml file and read its contents, parses to EmailMessage.

    :param eml_path: path to an eml file
    :return: parsed EmailMessage from file
    """
    with eml_path.open(encoding="utf-8", errors="replace") as file:
        try:
            # the policy 'strict' makes this return an EmailMessage class (Python 3.6+), rather than a Message class.
            # noinspection PyTypeChecker
            email_message: EmailMessage = message_from_file(file, policy=strict)  # type: ignore
            email_message.add_header("Original-Path", file.name)

            return email_message
        except MessageDefect:
            print(f"Could not parse file {str(eml_path)} to EmailMessage")
            return None


def read_messages_from_directory(directory_path: Union[Path, str]) -> List[EmailMessage]:
    """
    Read in eml files as email messages from a directory.

    :param directory_path: path to a directory containing eml file
    :return: list of email messages
    """
    email_messages: List[EmailMessage] = []

    for file_path in list_files_in_folder(directory_path):
        potential_message: Optional[EmailMessage] = read_message_from_file(file_path)
        if potential_message:
            email_messages.append(potential_message)

    return email_messages
