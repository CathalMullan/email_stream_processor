"""
Test - parse_email_messages.py.
"""
from email.message import EmailMessage
from pathlib import Path
from typing import List, Optional

import pytest

from email_stream_processor.helpers.globals.directories import TESTS_EMAIL_DIR, list_files_in_folder
from email_stream_processor.helpers.input.input_eml import read_message_from_file, read_message_from_string

VALID_EML_STR = """
Mime-Version: 1.0 (Apple Message framework v730)
Content-Type: multipart/mixed; boundary=Apple-Mail-13-196941151
Message-Id: <9169D984-4E0B-45EF-82D4-8F5E53AD7012@example.com>
From: foo@example.com
Subject: testing
Date: Mon, 6 Jun 2005 22:21:22 +0200
To: blah@example.com


--Apple-Mail-13-196941151
Content-Transfer-Encoding: quoted-printable
Content-Type: text/plain;
charset=ISO-8859-1;
delsp=yes;
format=flowed

This is the first part.

--Apple-Mail-13-196941151
Content-Type: image/jpeg
Content-Transfer-Encoding: base64
Content-Location: Photo25.jpg
Content-ID: <qbFGyPQAS8>
Content-Disposition: inline

jamisSqGSIb3DQEHAqCAMIjamisxCzAJBgUrDgMCGgUAMIAGCSqGSjamisEHAQAAoIIFSjCCBUYw
ggQujamisQICBD++ukQwDQYJKojamisNAQEFBQAwMTELMAkGA1UEBhMCRjamisAKBgNVBAoTA1RE
QzEUMBIGjamisxMLVERDIE9DRVMgQ0jamisNMDQwMjI5MTE1OTAxWhcNMDYwMjamisIyOTAxWjCB
gDELMAkGA1UEjamisEsxKTAnBgNVBAoTIEjamisuIG9yZ2FuaXNhdG9yaXNrIHRpbjamisRuaW5=

--Apple-Mail-13-196941151--
"""


def test_list_files_in_folder() -> None:
    """
    Ensure returns only files.

    :return: None
    """
    file_paths: List[Path] = list_files_in_folder(folder_path=TESTS_EMAIL_DIR)
    for file_path in file_paths:
        assert file_path.is_file()


@pytest.mark.parametrize("eml_path", list_files_in_folder(folder_path=TESTS_EMAIL_DIR))
def test_read_message_from_file(eml_path: Path) -> None:
    """
    Parse a string to an EmailMessage.

    :param eml_path: path to an eml file
    :return: None
    """
    email_message: Optional[EmailMessage] = read_message_from_file(eml_path=eml_path)

    if email_message:
        assert isinstance(email_message, EmailMessage)
    else:
        assert email_message is None


@pytest.mark.parametrize("message_str", [VALID_EML_STR])
def test_read_message_from_string(message_str: str) -> None:
    """
    Open a eml file and read its contents, parses to EmailMessage.

    :param message_str: eml file as a string
    :return: None
    """
    email_message: Optional[EmailMessage] = read_message_from_string(message_str=message_str)

    if email_message:
        assert isinstance(email_message, EmailMessage)
