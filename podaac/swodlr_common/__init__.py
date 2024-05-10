import logging as pylogging
from .logging import JsonFormatter

stream_handler = pylogging.StreamHandler()
stream_handler.setFormatter(JsonFormatter())

print(pylogging.root)
pylogging.basicConfig(
    handlers=(stream_handler,),
    force=True
)
