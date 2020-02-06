"""
Download the spaCy model used for anonymizer.
"""
import spacy


def main() -> None:
    """
    Download the spaCy model used for anonymizer.

    :return: None
    """
    spacy.cli.download(model="en_core_web_sm")
