"""
Test - address_validation.py.
"""
from typing import Optional

import pytest

from email_stream_processor.helpers.validation.address_validation import parse_address_str


@pytest.mark.parametrize(
    "address_header, valid_address",
    [
        ("someone@yahoo.com", True),
        ('"Grants-Notification" <infoz@reactive-outpost.com>\'', True),
        ("Louise </O=ENRON/OU=NA/CN=RECIPIENTS/CN=LKITCHEN>", False),
        ("<\"'l-bene'@cornellcollege.com'\"@enron.com>", True),
        ("", False),
    ],
)
def test_parse_address_str(address_header: str, valid_address: bool) -> None:
    """
    Verify addresses are parsed correctly.

    :param address_header: a string potentially containing an email address
    :param valid_address: whether a valid email exists within the address_header
    :return: None
    """
    parsed_address: Optional[str] = parse_address_str(potential_address=address_header)

    if valid_address:
        assert isinstance(parsed_address, str)
    else:
        assert parsed_address is None
