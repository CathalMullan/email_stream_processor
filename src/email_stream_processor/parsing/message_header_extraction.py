"""
Functions to extract contents from message headers.
"""
from datetime import datetime
from email.header import decode_header
from email.message import EmailMessage
from email.utils import make_msgid, mktime_tz, parsedate_tz, unquote
from encodings.aliases import aliases
from typing import Dict, List, Optional, Tuple

from email_stream_processor.helpers.anonymization.text_anonymizer import (
    faker_generate_replacements,
    hash_address_header,
    spacy_anonymize_text,
)
from email_stream_processor.helpers.config.get_config import CONFIG
from email_stream_processor.helpers.globals.regex import SUBJECT_PREFIX
from email_stream_processor.helpers.validation.address_validation import parse_address_str


def get_message_address(header_str: str) -> Optional[str]:
    """
    Get a message header as a single valid address string.

    :param header_str: the message header as a string
    :return: optional valid address from the message header
    """
    if not header_str:
        return None

    parsed_address: Optional[str] = parse_address_str(potential_address=header_str)
    if not parsed_address:
        return None

    if CONFIG.do_address_hashing:
        parsed_address = hash_address_header(parsed_address)

    return parsed_address


def get_message_address_list(header_str: str) -> Optional[List[str]]:
    """
    Get a message header as a list of valid address strings.

    :param header_str: the message header as a string
    :return: optional list of valid addresses strings from the message header
    """
    if not header_str:
        return None

    split_header_str: List[str] = header_str.split(", ")

    parsed_header_addresses: List[str] = []
    for potential_address in split_header_str:
        parsed_address: Optional[str] = parse_address_str(potential_address=potential_address)
        if isinstance(parsed_address, str):
            if CONFIG.do_address_hashing:
                parsed_address = str(hash_address_header(parsed_address))

            parsed_header_addresses.append(parsed_address)

    if not parsed_header_addresses:
        return None

    return parsed_header_addresses


def get_message_date(date_header_str: str) -> Optional[datetime]:
    """
    Get the message date header as a datetime.

    :param date_header_str: the message date header as a string
    :return: optional datetime from the date_header
    """
    if not date_header_str:
        return None

    date_header_tuple = parsedate_tz(date_header_str)
    if date_header_tuple:
        # Parse to UTC datetime
        date_timestamp = mktime_tz(date_header_tuple)

        date_datetime: Optional[datetime] = datetime.fromtimestamp(date_timestamp)
        if date_datetime:
            return date_datetime

    return None


def get_message_subject(subject_header_str: str) -> str:
    """
    Get the message subject header as a cleaned string.

    READING:
        * List of potential tags
        https://en.wikipedia.org/wiki/List_of_email_subject_abbreviations

    :param subject_header_str: the message subject header as a string
    :return: clean subject
    """
    if not subject_header_str:
        return ""

    # Remove tagging prefixes such as 'Re' and 'Fwd' using regex.
    subject = str(SUBJECT_PREFIX.sub("", subject_header_str))

    # Identify personal information
    if CONFIG.do_content_tagging:
        subject = spacy_anonymize_text(subject)

        # Anonymize personal information
        if CONFIG.do_faker_replacement:
            subject = faker_generate_replacements(subject)

    return subject


def get_message_message_id(message_id_str: str) -> str:
    """
    Get the message message-id header as a string.

    NOTE: No need to use unquote, as policy strict bakes this in.

    :param message_id_str: the message 'message id' header as a string
    :return: parsed or generated message id
    """
    # Create message-id if non found
    if not message_id_str:
        message_id_str = make_msgid()

    clean_message_id = unquote(message_id_str)

    if CONFIG.do_address_hashing:
        clean_message_id = hash_address_header(clean_message_id)

    return clean_message_id


def get_message_raw_headers(message: EmailMessage) -> Optional[Dict[str, str]]:
    """
    Extract the header keys and values from an email message.

    Handle parsing errors that are common with individual headers.

    :param message: a parsed EmailMessage
    :return: an optional dictionary of header keys and values
    """
    raw_headers: Dict[str, str] = {}

    # Accessing raw headers using items() can hang due to invalid charsets.
    header_list: List[Tuple[str, str]] = list(message.raw_items())  # type: ignore

    for header_key, header_value in header_list:
        if header_key.lower() in ["message-id", "date", "from", "to", "cc", "bcc", "subject"]:
            header_string: str = parse_header_value(header_value=header_value)
            raw_headers[header_key.lower()] = header_string

    all_headers = dict(raw_headers)

    # Ensure headers list contains 'To', 'From' and 'Subject'
    if not all(header in all_headers.keys() for header in ["to", "from", "subject"]):
        return None

    return all_headers


def parse_header_value(header_value: str) -> str:
    """
    Email header to be parsed and decoded to string.

    :param header_value: header value as string
    :return: parsed decoded header value
    """
    for value, charset in decode_header(header_value):
        if charset:
            # Check charset is a valid Python charset
            clean_charset = charset.replace("-", "_")
            if clean_charset and clean_charset in aliases.keys():
                return str(value, encoding=clean_charset, errors="replace")
        else:
            # Convert bytes to string
            if isinstance(value, bytes):
                return value.decode(errors="replace")

    return str(header_value)
