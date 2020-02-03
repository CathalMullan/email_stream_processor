"""
Validation around email addresses.
"""
from email.utils import parseaddr
from typing import Optional, Tuple

import validators


def parse_address_str(potential_address: str) -> Optional[str]:
    """
    Parse out email address from a string and verifies it is valid.

    :param potential_address: string containing a potential email address
    :return: optional parsed address
    """
    address_tuple: Tuple[str, str] = parseaddr(potential_address)
    address_strip = address_tuple[1].strip()

    if not validators.email(address_strip):
        return None

    return address_strip
