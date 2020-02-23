"""
Functions to extract contents from message body.
"""
from email.message import EmailMessage
from typing import List, Optional

from bs4 import BeautifulSoup
from talon import quotations

from email_stream_processor.helpers.anonymization.text_anonymizer import (
    faker_generate_replacements,
    spacy_anonymize_text,
)
from email_stream_processor.helpers.config.get_config import CONFIG
from email_stream_processor.helpers.globals.regex import INLINE_MESSAGE_SEPARATOR, SUSPICIOUS_INLINE
from email_stream_processor.helpers.validation.text_validation import is_valid_length, strip_html_contents


def get_message_body(message: EmailMessage) -> Optional[str]:
    """
    Get the core message body part as a cleaned string.

    In terms of the goal of the project, we are interested in unique unstructured text of a reasonable length.
    So we discard forwarded/replied emails, and those which are too short/long.

    NOTE: As part of the identification of actionable emails, the above is subject to change, but for now the
    presumption is otherwise.

    The Enron dataset used for testing seems to have quite a number of poorly parsed email bodies.
    Issues such as incorrect splitting of urls across lines cause havoc without attempts to clean the data.
    Will have to make due for now, but some 'strange' parsing seem here is a direct result of the data used.

    READING:
        * General discussion on the topic.
        https://en.wikipedia.org/wiki/Posting_style

        * MailGun sited these papers for their 'Talon' project.
        http://www.cs.cmu.edu/~vitor/papers/sigFilePaper_finalversion.pdf
        http://www.cs.cornell.edu/people/tj/publications/joachims_01a.pdf

    :param message: a parsed EmailMessage
    :return: cleaned message body as a string
    """
    core_message: Optional[EmailMessage] = message.get_body()  # type: ignore
    if not isinstance(core_message, EmailMessage):
        return None

    potential_message_body: Optional[str] = extract_core_message_body(core_message)
    if not potential_message_body or not isinstance(potential_message_body, str):
        return None

    # Raw body as a string
    message_body: str = potential_message_body

    # Remove inline mails
    message_body = remove_inline_message(message_body)

    # Check length now to prevent unnecessary processing
    if not is_valid_length(text=message_body, minimum=250, maximum=5_000):
        return None

    # Handle HTML in text
    if "html" in core_message.get_content_subtype() or bool(BeautifulSoup(message_body, "html.parser").find()):
        message_body = strip_html_contents(text=message_body)

    # Use Talon to attempt to remove message quotations
    message_body = str(quotations.extract_from_plain(message_body))

    # Identify personal information
    if CONFIG.do_content_tagging:
        message_body = spacy_anonymize_text(message_body)

        # Anonymize personal information
        if CONFIG.do_faker_replacement:
            message_body = faker_generate_replacements(message_body)

    return message_body


def extract_core_message_body(message: EmailMessage) -> Optional[str]:
    """
    Given the core message instance, walk the parts tree to collect message body as text.

    :param message: a parsed EmailMessage
    :return: message body part as string
    """
    try:
        if message.is_multipart():
            for part in message.walk():
                if part.get_content_type() == "text/plain" and "attachment" not in str(part.get("content-disposition")):
                    return str(part.get_content())  # type: ignore
        else:
            return str(message.get_content())
    except (KeyError, LookupError):
        return None

    return None


def remove_inline_message(message_body: str) -> str:
    """
    Attempt to identify inline messages in email body.

    :param message_body: a message body string
    :return: message body string without inline messages
    """
    clean_mail: List[str] = []

    for line in message_body.splitlines():
        if INLINE_MESSAGE_SEPARATOR.search(line):
            break

        if SUSPICIOUS_INLINE.search(line):
            break

        clean_mail.append(line)

    clean_mail_str = "\n".join(clean_mail)
    return clean_mail_str
