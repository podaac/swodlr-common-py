'''Useful logging utilities and helpers'''
from logging import LoggerAdapter


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
