import logging as pylogging
from .logging import JsonFormatter

for handler in pylogging.getLogger().handlers:
    handler.setFormatter(JsonFormatter())
