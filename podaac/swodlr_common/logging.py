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
        kwargs['extra'] = {
            'product_id': self._job.get('product_id'),
            'job_id': self._job.get('job_id')
        }

        return (msg, kwargs)

class LaxJsonEncoder(json.JSONEncoder):
    '''
    Subclassed JSON encoder which takes anything unserializable and attempts to
    serialize into a string. Otherwise, returns a typeerror
    '''
    
    def default(self, o):
        try:
            return str(o)
        except:
            self.default(o)


class JsonFormatter(Formatter):
    '''
    A Formatter subclass which provides JSON-readable log formats for easier
    ingestion and parsing
    '''

    def format(self, record: LogRecord, _datefmt=None):
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

        for key in ('product_id', 'job_id'):
            if hasattr(record, key):
                output.update(**{key: getattr(record, key)})

        return json.dumps(output, cls=LaxJsonEncoder)
