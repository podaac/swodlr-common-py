import logging as pylogging
from .logging import JsonFormatter

stream_handler = pylogging.StreamHandler()
stream_handler.setFormatter(JsonFormatter())

for handler in pylogging.getLogger().handlers:
    handler.setFormatter(JsonFormatter())
