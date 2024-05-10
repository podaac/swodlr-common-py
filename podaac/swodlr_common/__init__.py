import logging
from podaac.swodlr_common.logging import JsonFormatter

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(JsonFormatter)

logging.basicConfig(
    stream_handler=stream_handler
)
