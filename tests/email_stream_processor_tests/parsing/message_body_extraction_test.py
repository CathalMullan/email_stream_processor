"""
Test - message_body_extraction.py.
"""
from email.message import EmailMessage
from typing import Optional

import pytest

from email_stream_processor.helpers.globals.directories import TESTS_EMAIL_DIR
from email_stream_processor.helpers.input.input_eml import read_messages_from_directory
from email_stream_processor.parsing.message_body_extraction import get_message_body


@pytest.mark.parametrize("message", read_messages_from_directory(TESTS_EMAIL_DIR))
def test_get_message_body(message: EmailMessage) -> None:
    """
    Ensure we can extract the body of an array of email messages.

    :param message: a parsed EmailMessage
    :return: None
    """
    message_body: Optional[str] = get_message_body(message=message)

    if message_body is not None:
        assert isinstance(message_body, str)
    else:
        assert message_body is None
