"""
Parse environment into a config class.
"""
from dataclasses import dataclass
from os import getenv
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from email_stream_processor.helpers.globals.directories import PROJECT_DIR

load_dotenv(dotenv_path=f"{PROJECT_DIR}/.env")


def is_true(variable: Optional[str]) -> bool:
    """
    Validate string comparision to handle boolean environment variables.

    :param variable: environment variable
    :return: boolean if 'true'
    """
    return variable == "true"


@dataclass
class Config:
    """
    Environment variable mapping object.

    Allows greater control over centralised typing of environment variables.
    """

    # Generic
    is_dev: bool = is_true(getenv("IS_DEV"))

    # Message Extraction
    do_content_tagging: bool = is_true(getenv("DO_CONTENT_TAGGING"))
    do_faker_replacement: bool = is_true(getenv("DO_FAKER_REPLACEMENT"))
    do_address_hashing: bool = is_true(getenv("DO_ADDRESS_HASHING"))

    # Google Cloud
    gcp_credentials: Path = Path(str(getenv("GCP_CREDENTIALS")))

    # Kafka
    kafka_hosts: str = str(getenv("KAFKA_HOSTS"))
    kafka_topic: str = str(getenv("KAFKA_TOPIC"))

    # Bucket
    bucket_parquet: str = str(getenv("BUCKET_PARQUET"))
    bucket_checkpoint: str = str(getenv("BUCKET_CHECKPOINT"))


CONFIG = Config()
