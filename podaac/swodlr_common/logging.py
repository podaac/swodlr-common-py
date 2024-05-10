'''Useful logging utilities and helpers'''
import json
from logging import Formatter, LogRecord, LoggerAdapter


class JobMetadataInjector(LoggerAdapter):
    '''
    Wraps a logging.Logger object and injects job metadata into each log
    message
    '''

    def __init__(self, logger, job):
        super().__init__(logger, None)
        self._job = job

    def process(self, msg, kwargs):
        if isinstance(msg, str):
            return (
                '[product_id: {}, job_id: {}] {}'.format(  # pylint: disable=consider-using-f-string # noqa: E501
                    self._job.get('product_id'),
                    self._job.get('job_id'),
                    msg
                ),
                kwargs
            )

        return (msg, kwargs)

class JsonFormatter(Formatter):
    def format(self, record: LogRecord, datefmt=None):
        timestamp = self.formatTime(record)
        level = record.levelname
        message = record.getMessage()

        output = {
            'timestamp': timestamp,
            'level': level,
            'message': message
        }

        if record.exc_info:
            output.update(exception=record.exc_info)

        if record.stack_info:
            stack = self.formatStack(record.stack_info)
            output.update(stack=stack)

        if record.args:
            output.update(args=record.args)

        return json.dumps(output)
