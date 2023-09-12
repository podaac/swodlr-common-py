'''A helper class for creating microservices which handle jobset schemas'''
from abc import ABC, abstractmethod
from copy import deepcopy
import json
from time import sleep
import traceback

from fastjsonschema import JsonSchemaException

from podaac.swodlr_common.utilities import BaseUtilities


class JobHandler(ABC):
    '''
    The base class of lambdas which are to handle job inputs
    '''

    def __init__(self, utilities: BaseUtilities):
        self._max_attempts = utilities.get_param('max_tries') \
            if utilities.get_param('max_tries') is not None else 3

        self._backoff_factor = utilities.get_param('backoff_factor') \
            if utilities.get_param('backoff_factor') is not None else 2

        self._logger = utilities.get_logger(__name__)
        self._validate_jobset = utilities.load_json_schema('jobset')

    def invoke(self, event, _context):
        '''
        Method to be invoked by the lambda runtime. Performs validation of both
        input and output data while deferring to the handle_jobs implementation
        to handle the jobs themselves
        '''
        records = event['Records']
        self._logger.debug('Records received: %d', len(records))

        input_jobs = []
        for record in records:
            try:
                body = self._validate_jobset(json.loads(record['body']))
                input_jobs.extend(body['jobs'])
            except JsonSchemaException:
                self._logger.exception('Error validating input jobset')
                self._logger.error(record['body'])

        output_jobs = self.handle_jobs(input_jobs)

        try:
            job_set = self._validate_jobset({'jobs': output_jobs})
            return job_set
        except JsonSchemaException:
            self._logger.exception('Error validating output jobset')
            return None

    def handle_jobs(self, input_jobs):
        '''
        Handler that takes the raw jobs array and processes them individually
        with handle_job. Can be overwritten in services to handle jobs in batch
        '''

        output_jobs = [
            self._try_handle_job(input_job) for input_job in input_jobs
        ]
        return output_jobs

    def _try_handle_job(self, input_job):
        '''
        Calls handle_job and gracefully handles any exceptions that are raised
        by performing exponential backoffs. If max_attempts is reached, a job
        with a failed status and traceback is produced
        '''

        for duration in [i**2 for i in range(0, self._max_attempts + 1)]:
            if duration > 0:
                self._logger.info(f'Backing off for {duration} seconds')
                sleep(duration)
            try:
                return self.handle_job(deepcopy(input_job))
            except Exception:  # pylint: disable=broad-exception-caught
                self._logger.exception(
                    'Exception occurred while running job handler'
                )

        # handle_job surpassed allowed max attempts
        output_job = deepcopy(input_job)

        if not hasattr(output_job, 'errors'):
            output_job['errors'] = []
        output_job['job_status'] = 'job-failed'
        output_job['traceback'] = traceback.format_exc()
        output_job['errors'].append('SDS pipeline failed')

        return output_job

    @abstractmethod
    def handle_job(self, job):
        '''
        Handler which receives individual job objects and performs processing
        on them. Should be implemented by subclasses
        '''
