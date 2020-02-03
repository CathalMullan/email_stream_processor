"""
Test - text_validation.py.
"""
from email_stream_processor.helpers.validation.text_validation import strip_html_contents

SENTENCE = "Here is a string."
ADDRESS = "123 Main St. Nashville, TN 37212"

HTML_TEXT = f"""
<table>
    <tr>
        <td align="center"><img src="logo.jpg"></td>
    </tr>
    <tr>
        <td>{SENTENCE}</td>
    </tr>
    <tr>
        <td align="center">{ADDRESS}</td>
    </tr>
</table>
"""


def test_strip_html_contents() -> None:
    """
    Test the use of bs4 to strip HTML tags. Ensure we get the important text stored with the HTML.

    :return: None
    """
    cleaned_text = strip_html_contents(text=HTML_TEXT)

    # Ensure no HTML chars are in the cleaned text
    html_chars = ["<", ">"]
    assert any(char in cleaned_text for char in html_chars) is False

    # Ensure correct strings extracted from HTML
    contents = [SENTENCE, ADDRESS]
    assert all(string in cleaned_text for string in contents) is True
