"""
Test - message_header_extraction.py.
"""
from datetime import datetime
from typing import List, Optional

import pytest

from email_stream_processor.parsing.message_header_extraction import (
    get_message_address,
    get_message_address_list,
    get_message_date,
    get_message_message_id,
    get_message_subject,
)


@pytest.mark.parametrize(
    "header_str, valid_address",
    [
        ("Jack McGuinness", False),
        (None, False),
        ("", False),
        ("Louise </O=ENRON/OU=NA/CN=RECIPIENTS/CN=LKITCHEN>", False),
        ("<\"'l-bene'@cornellcollege.com'\"@enron.com>", True),
    ],
)
def test_get_message_address(header_str: Optional[str], valid_address: bool) -> None:
    """
    Ensure a single address can be extracted from a string.

    :param header_str: the message header as a string
    :param valid_address: whether a valid address exists in the header_str
    :return: None
    """
    message_address: Optional[str] = get_message_address(header_str=header_str)

    if valid_address:
        assert isinstance(message_address, str)
    else:
        assert message_address is None


@pytest.mark.parametrize(
    "header_str, valid_address_count",
    [
        ("user1@company1.com, John Doe <user2@example.com>", 2),
        (None, 0),
        ("", 0),
        ("<Louise </O=ENRON/OU=NA/CN=RECIPIENTS/CN=LKITCHEN>", 0),
        ("Sally Goodall Smith <sally@foo.com>, <Louise </O=ENRON/OU=NA/CN=RECIPIENTS/CN=LKITCHEN>", 1),
        ("<\"'l-bene'@cornellcollege.com'\"@enron.com>", 1),
    ],
)
def test_get_message_address_list(header_str: Optional[str], valid_address_count: int) -> None:
    """
    Ensure a number of address can be extracted from a comma separated string.

    :param header_str: the message header as a string
    :param valid_address_count: the amount of valid addresses in the string (as per RFC 822)
    :return: None
    """
    message_address_lit: Optional[List[str]] = get_message_address_list(header_str=header_str)

    if not message_address_lit:
        assert valid_address_count == 0
    else:
        count_addresses = len(message_address_lit)
        assert valid_address_count == count_addresses


@pytest.mark.parametrize(
    "date_header_str, valid_date",
    [
        ("Fri, 24 Aug 2012 08:36:35 UT", True),
        (None, False),
        ("Pn, 29 paX 2007 21:13:00 +0100", False),
        ("", False),
        ("<\"'l-bene'@cornellcollege.com'\"@enron.com>", False),
    ],
)
def test_get_message_date(date_header_str: Optional[str], valid_date: bool) -> None:
    """
    Ensure the message date can be extracted from an string, and parsed to datetime.

    :param date_header_str: the message date header as a string
    :param valid_date: whether the date_header_str is valid (as per RFC 7231)
    :return: None
    """
    message_date: Optional[datetime] = get_message_date(date_header_str=date_header_str)

    if valid_date:
        assert isinstance(message_date, datetime)
    else:
        assert message_date is None


@pytest.mark.parametrize(
    "subject_header_str", [None, "", "Re: Your job offer...", "Fwd:", "Dit is een Nederlandse verklaring"]
)
def test_get_message_subject(subject_header_str: str) -> None:
    """
    Ensure the subject header of a mail can be parsed and verified as English.

    :param subject_header_str: the message subject header as a string
    :return:
    """
    message_subject: str = get_message_subject(subject_header_str=subject_header_str)
    assert isinstance(message_subject, str)


@pytest.mark.parametrize("message_id_str", ["Message-ID: <78910@example.net>", None, ""])
def test_get_message_message_id(message_id_str: str) -> None:
    """
    Ensure the message id can be extracted or created if not exists.

    :param message_id_str: the message message id header as a string
    :return: None
    """
    message_message_id: str = get_message_message_id(message_id_str=message_id_str)
    assert isinstance(message_message_id, str)
