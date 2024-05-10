import logging as pylogging
from .logging import JsonFormatter

stream_handler = pylogging.StreamHandler()
stream_handler.setFormatter(JsonFormatter())

pylogging.basicConfig(
    handlers=(stream_handler,)
)
