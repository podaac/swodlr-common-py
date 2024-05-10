import logging
from .logging import JsonFormatter

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(JsonFormatter)

logging.basicConfig(
    stream_handler=stream_handler
)
