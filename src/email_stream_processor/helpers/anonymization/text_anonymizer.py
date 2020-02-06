"""
Identify and anonymize personally identifiable information in text.

https://spacy.io/api/annotation.
https://faker.readthedocs.io/en/master/providers.html
"""
import binascii
import hashlib
from typing import Callable, Dict

import faker
import spacy
from faker.providers import address, company, currency, internet, person, python

# See https://blog.dominodatalab.com/making-pyspark-work-spacy-overcoming-serialization-errors/
SPACY_TAGGER = spacy.load("en_core_web_sm")

FAKE = faker.Faker(locale="en_US")
FAKE.add_provider([internet, person, company, currency, address, python])

FAKER_DICT: Dict[str, Callable[[], str]] = {
    # People, including fictional.
    "PERSON": lambda: str(FAKE.first_name()),
    # Nationalities or religious or political groups.
    "NORP": lambda: str(FAKE.country()),
    # Buildings, airports, highways, bridges, etc.
    "FAC": lambda: str(FAKE.country()),
    # Companies, agencies, institutions, etc.
    "ORG": lambda: str(FAKE.last_name_male()),
    # Countries, cities, states.
    "GPE": lambda: str(FAKE.country()),
    # Non-GPE locations, mountain ranges, bodies of water.
    "LOC": lambda: str(FAKE.country()),
    # Objects, vehicles, foods, etc. (Not services.)
    "PRODUCT": lambda: str(FAKE.company()),
    # Named hurricanes, battles, wars, sports events, etc.
    "EVENT": lambda: str(FAKE.name()),
    # Titles of books, songs, etc.
    "WORK_OF_ART": lambda: str(FAKE.name()),
    # Named documents made into laws.
    "LAW": lambda: str(FAKE.name()),
    # Any named language.
    "LANGUAGE": lambda: str(FAKE.name()),
    # Absolute or relative dates or periods.
    "DATE": lambda: str(FAKE.month_name()),
    # Times smaller than a day.
    "TIME": lambda: str(FAKE.time(pattern="%H:%M:%S", end_datetime=None)),
    # Percentage, including ”%“.
    "PERCENT": lambda: str(FAKE.name()),
    # Monetary values, including unit.
    "MONEY": lambda: str(FAKE.currency_name()),
    # Measurements, as of weight or distance.
    "QUANTITY": lambda: str(FAKE.pyint()),
    # “first”, “second”, etc.
    "ORDINAL": lambda: str(FAKE.pyint()),
    # Numerals that do not fall under another type.
    "CARDINAL": lambda: str(FAKE.pyint()),
    # Does the token resemble an email address?
    "EMAIL": lambda: str(FAKE.safe_email()),
    # Does the token resemble a URL?
    "URL": lambda: str(FAKE.url()),
    # Does the token represent a number? e.g. “10.9”, “10”, “ten”, etc.
    "NUM": lambda: str(FAKE.pyint()),
}


def spacy_anonymize_text(text: str) -> str:
    """
    Use spaCy to find and replace entities with their tag.

    :param text: text to be cleaned
    :return: text with entities removed
    """
    doc = SPACY_TAGGER(text)

    for entity in sorted(doc.ents, key=lambda ent: len(ent.text), reverse=True):
        text = text.replace(str(entity.text).strip(), f"{entity.label_}")

    for token in doc:
        if token.like_email:
            text = text.replace(str(token.text).strip(), f"EMAIL")
        elif token.like_url:
            text = text.replace(str(token.text).strip(), f"URL")
        elif token.like_num:
            text = text.replace(str(token.text).strip(), f"NUM")

    return text


def faker_generate_replacements(text: str) -> str:
    """
    Replace tags in text with generated fake data.

    :param text: text with spaCy tags
    :return: text with Faker generated replacements for spaCy tags
    """
    parsed_text = SPACY_TAGGER(text, disable=["parser", "tagger", "ner"])

    text_tokens = [token.text for token in parsed_text]
    spacy_tags = list(FAKER_DICT.keys())

    for token in text_tokens:
        for key in spacy_tags:
            if key in token:
                text = text.replace(token, FAKER_DICT[key](), 1)

    return text


def hash_salt_text(text: str) -> str:
    """
    Take a string and returns a hashed/salted version using secret key.

    :param text: string to be hashed
    :return: hashed version of string
    """
    salt = b"f8bfae8c3aee9de3e72e683b948fea550aa2fd3dbae4bd2d4912def1b3c7f90f51e5e6cafc70bc6daa2df29cf96f319c6ed50e9"

    hashed_str: str = binascii.hexlify(
        hashlib.pbkdf2_hmac(hash_name="sha512", password=text.encode("utf-8"), salt=salt, iterations=1, dklen=8)
    ).decode("utf-8")

    return hashed_str


def hash_address_header(header_str: str) -> str:
    """
    Replace address with hashed/salted alternative, split on "@".

    :param header_str: an address header
    :return: the same address header hashed as a string
    """
    local, at_symbol, domain = header_str.rpartition("@")

    local_clean = hash_salt_text(local)
    domain_clean = hash_salt_text(domain)

    return local_clean + at_symbol + domain_clean
