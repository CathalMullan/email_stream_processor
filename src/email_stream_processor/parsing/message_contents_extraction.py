"""
Extract headers and body from email message.
"""
import os
from dataclasses import dataclass, field
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql.types import StringType, StructField, StructType

from email_stream_processor.helpers.globals.directories import ENRON_DIR
from email_stream_processor.helpers.input.input_eml import read_message_from_file, read_message_from_string
from email_stream_processor.helpers.validation.text_validation import is_valid_length
from email_stream_processor.parsing.message_body_extraction import get_message_body
from email_stream_processor.parsing.message_header_extraction import (
    get_message_address,
    get_message_address_list,
    get_message_date,
    get_message_message_id,
    get_message_raw_headers,
    get_message_subject,
)


@dataclass
class MessageContent:
    """
    Select components and headers of a parsed EmailMessage.
    """

    original_path: Optional[str] = None
    message_id: str = ""
    date: Optional[datetime] = None
    from_address: str = ""
    to_address_list: List[str] = field(default_factory=list)
    cc_address_list: Optional[List[str]] = None
    bcc_address_list: Optional[List[str]] = None
    subject: str = ""
    body: str = ""

    def validate(self) -> bool:
        """
        Verify if MessageContents instance is valid for further processing.

        :return: bool whether message_contents is valid
        """
        # # Require a valid message-id (generated or otherwise)
        # if not self.message_id:
        #     print("Invalid message-id.")
        #     return False
        #
        # # Require a valid from address
        # if not self.from_address:
        #     print("No 'From' address")
        #     return False
        #
        # # Require at least one valid to address
        # if not self.to_address_list or all(address == "" for address in self.to_address_list):
        #     print("No 'To' address.")
        #     return False

        # Require a message body of reasonable length
        if not self.body or not is_valid_length(text=self.body, minimum=200, maximum=50_000):
            print("Invalid body length.")
            return False

        return True

    def address_list_to_str(self, address_list: str) -> str:
        """
        Concatenate address list into a comma separated list.

        :return: string representation of to_address_list
        """
        if address_list not in ("to_address_list", "cc_address_list", "bcc_address_list"):
            return ""

        address_list_variable = self.__getattribute__(address_list)

        if not address_list_variable or all(address == "" for address in address_list_variable):
            return ""

        return ", ".join(address_list_variable).strip()

    def as_str(self) -> str:
        """
        Convert MessageContents instance into an eml like string.

        :return: eml file of message
        """
        return f"""
Message-Id: {self.message_id}
Date: {self.date}
From: {self.from_address}
To: {self.address_list_to_str('to_address_list')}
Cc: {self.address_list_to_str('cc_address_list')}
Bcc: {self.address_list_to_str('bcc_address_list')}
Subject: {self.subject}

{self.body}
""".strip()

    def as_dict(self) -> Dict[str, str]:
        """
        Convert MessageContents instance into a dict.

        :return: dict of contents
        """
        return {
            "message_id": self.message_id,
            "date": str(self.date),
            "from_address": self.from_address,
            "to_addresses": self.address_list_to_str("to_address_list"),
            "cc_addresses": self.address_list_to_str("cc_address_list"),
            "bcc_addresses": self.address_list_to_str("bcc_address_list"),
            "subject": self.subject,
            "body": self.body,
        }


def extract_message_contents(message: EmailMessage) -> Optional[MessageContent]:
    """
    Extract fields from a message to a dict of contents.

    TODO: Consider hashing to prevent duplicates?

    :param message: a parsed EmailMessage
    :return: optional parsed message content
    """
    # Validate required headers are present.
    raw_headers: Optional[Dict[str, str]] = get_message_raw_headers(message=message)
    if not raw_headers:
        print("Invalid headers.")
        return None

    # Build message contents
    message_contents = MessageContent(
        message_id=get_message_message_id(message_id_str=raw_headers.get("message-id")),
        date=get_message_date(date_header_str=raw_headers.get("date")),
        from_address=get_message_address(header_str=raw_headers.get("from")),
        to_address_list=get_message_address_list(header_str=raw_headers.get("to")),
        cc_address_list=get_message_address_list(header_str=raw_headers.get("cc")),
        bcc_address_list=get_message_address_list(header_str=raw_headers.get("bcc")),
        subject=get_message_subject(subject_header_str=raw_headers.get("subject")),
        body=get_message_body(message=message),
    )

    # Validate final state of message.
    # TODO: Consider using exceptions to align with Python standards.
    if not message_contents.validate():
        return None

    return message_contents


def eml_path_to_message_contents(eml_path: Path) -> Optional[MessageContent]:
    """
    Read eml file and convert to message content.

    :param eml_path: path to an eml file
    :return: optional message content
    """
    email_message: Optional[EmailMessage] = read_message_from_file(eml_path)
    if not isinstance(email_message, EmailMessage):
        return None

    message_contents: Optional[MessageContent] = extract_message_contents(email_message)
    if not isinstance(message_contents, MessageContent):
        return None

    print(f"Successfully parsed file {os.path.relpath(str(eml_path), f'{ENRON_DIR}/maildir')}", flush=True)
    return message_contents


# Representation of Message Contents Dict in Spark
MESSAGE_CONTENTS_STRUCT = StructType(
    [
        StructField(name="message_id", dataType=StringType(), nullable=True),
        StructField(name="date", dataType=StringType(), nullable=True),
        StructField(name="from_address", dataType=StringType(), nullable=True),
        StructField(name="to_addresses", dataType=StringType(), nullable=True),
        StructField(name="bcc_addresses", dataType=StringType(), nullable=True),
        StructField(name="subject", dataType=StringType(), nullable=True),
        StructField(name="body", dataType=StringType(), nullable=True),
    ]
)


def eml_str_to_spark_message_contents(eml_str: str) -> Optional[Dict[str, str]]:
    """
    Process eml file as bytes and convert to message content dict.

    :param eml_str: str representation of an eml file
    :return: optional message content
    """
    email_message: Optional[EmailMessage] = read_message_from_string(eml_str.strip())
    if not isinstance(email_message, EmailMessage):
        print(f"Could not parse string to EmailMessage")
        return None

    message_contents: Optional[MessageContent] = extract_message_contents(email_message)
    if not isinstance(message_contents, MessageContent):
        print(f"Could not parse string to MessageContent")
        return None

    message_contents_dict: Dict[str, str] = message_contents.as_dict()
    return message_contents_dict
