"""
Validation of generic text.
"""
import warnings

from bs4 import BeautifulSoup

# bs4 is very warning happy - suppress
warnings.filterwarnings("ignore", category=UserWarning, module="bs4")


def strip_html_contents(text: str) -> str:
    """
    Strip HTML tags from text, returning its contents.

    :param text: string containing HTML
    :return: stripped string with HTML removed
    """
    soup = BeautifulSoup(text, "lxml")

    stripped_text = str(soup.text.strip())
    return stripped_text


def is_valid_length(text: str, minimum: int, maximum: int) -> bool:
    """
    Ensure text is between a min and max.

    :param text: text to measure length
    :param minimum: lower bound integer
    :param maximum: higher bound integer
    :return: bool if valid
    """
    text_len = len(text)
    if text_len > maximum or text_len < minimum:
        return False

    return True
