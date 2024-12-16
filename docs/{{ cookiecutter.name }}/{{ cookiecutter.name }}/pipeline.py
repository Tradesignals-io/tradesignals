"""
{{ cookiecutter.friendly_name }} stream processing pipeline application for Tradesignals.io.
"""

import os
import logging
from dotenv import load_dotenv

from dotenv_derive import dotenv

from attrs import define
from cattrs import structure, unstructure

from {{ cookiecutter.name }}.lib.producer import AvroProducer
from {{ cookiecutter.name }}.lib.validators import validate_enum
from {{ cookiecutter.name }}.lib.types import AvroDecimalT, TimestampT, Bias, Side
import {{ cookiecutter.name }}.lib.databento_utils as bento_utils
import databento as db

logger = logging.getLogger({{ cookiecutter.name }})
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())
logger.addHandler(logging.FileHandler("{{ cookiecutter.name }}.log"))

load_dotenv()



class {{ cookiecutter.name.strip().capitalize() }}Pipeline(AvroProducer):

    def extract_value(self, data: Dict[str, Any] | Any, msg_id: int | str | None) -> Dict[str, Any]:
        record = {}
        return record


    def extract_price(self, msg: bytes) -> AvroDecimalT:
        return AvroDecimalT(msg.price)


def main():
    """
    Main entry point for the {{ cookiecutter.name }} pipeline.
    """
    REPLAY_LAST_24H = True
    DEBUG_MODE = True
    logger.info("Starting {{ cookiecutter.name }} pipeline...")
    pipeline = {{ cookiecutter.name.strip().capitalize() }}Pipeline(
        topic_name={{ cookiecutter.output_topic }},
        debug_mode=DEBUG_MODE,
        replay_last24h=REPLAY_LAST_24H,
        logger=logger
    )

    pipeline.run()


__all__ = ["main"]
