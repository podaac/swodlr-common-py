'''
Decorators utilized to declare job handlers and inject lambda_handlers
within service modules
'''
from copy import deepcopy
import json
import inspect
import sys
from time import sleep
import traceback
from weakref import WeakSet

from fastjsonschema import JsonSchemaException

from .utilities import BaseUtilities

loaded_modules = WeakSet()


def job_handler(handler):
    '''
    Decorator used to declare that a function is a job handler. The function
    MUST have one input arg for a job object and MUST return a job object with
    the same product_id. The decorator will handle automatic retrying of the
    handler upon unhandled exception. This decorator will also inject a
    lambda_handler into the module. Only one handler per module may be declared
    '''

    module = inspect.getmodule(handler)
    if module not in loaded_modules:
        loaded_modules.add(module)
    else:
        raise RuntimeError(
            'Multiple job handlers cannot be declared in one module'
        )

    # Inject our lambda_handler into the module
    module.lambda_handler = _generate_lambda_handler(
        _generate_bulk_job_handler(handler)
    )
    return handler


def bulk_job_handler(handler, returns_jobset=False):
    '''
    Decorator used to declare that a function is a bulk job handler. The
    function MUST have one input arg for an iterable of job objects and MUST
    return an iterable of job objects preserving the product_ids of each input
    job. This decorator will also inject a lambda_handler into the module.
    Only one handler per module may be declared

    :param handler: the handler function to wrap. should have one parameter
    :param returns_jobset: whether or not the function will return a jobset
                          itself (True) or an array of job objects (False)
    '''

    module = inspect.getmodule(handler)
    if module not in loaded_modules:
        loaded_modules.add(module)
    else:
        raise RuntimeError(
            'Multiple job handlers cannot be declared in one module'
        )

    # Inject our lambda_handler into the module
    module.lambda_handler = _generate_lambda_handler(handler, returns_jobset)
    return handler


def _generate_bulk_job_handler(handler):
    utils = BaseUtilities.get_instance()
    logger = utils.get_logger(__name__)
    max_attempts = utils.get_param('max_attempts')

    def try_handler(input_job):
        for duration in [i**2 for i in range(0, max_attempts + 1)]:
            if duration > 0:
                logger.info('Backing off for %d seconds', duration)
                sleep(duration)
            try:
                return handler(deepcopy(input_job))
            except Exception:  # pylint: disable=broad-exception-caught
                logger.exception(
                    'Exception occurred while running job handler'
                )

        # handle_job surpassed allowed max attempts
        output_job = deepcopy(input_job)

        exception = '\n'.join(traceback.format_exception(
            sys.last_type, sys.last_value, sys.last_traceback  # pylint: disable=no-member # noqa: E501
        ))

        if not hasattr(output_job, 'errors'):
            output_job['errors'] = []
        output_job['job_status'] = 'job-failed'
        output_job['traceback'] = exception
        output_job['errors'].append('SDS pipeline failed')

        return output_job

    def default_bulk_job_handler(input_jobs):
        output_jobs = [
            try_handler(input_job) for input_job in input_jobs
        ]
        return output_jobs

    return default_bulk_job_handler


def _generate_lambda_handler(handler, returns_jobset=False):
    utils = BaseUtilities.get_instance()
    logger = utils.get_logger(__name__)
    validate_jobset = utils.load_json_schema('jobset')

    def lambda_handler(event, _context):
        records = event['Records']
        logger.debug('Records received: %d', len(records))

        input_jobs = []
        for record in records:
            try:
                body = validate_jobset(json.loads(record['body']))
                input_jobs.extend(body['jobs'])
            except JsonSchemaException:
                logger.exception('Error validating input jobset')
                logger.error(record['body'])

        if returns_jobset:
            output_jobset = handler(input_jobs)
        else:
            output_jobs = handler(input_jobs)
            output_jobset = {'jobs': output_jobs}

        try:
            jobset = validate_jobset(output_jobset)
            return jobset
        except JsonSchemaException:
            logger.exception('Error validating output jobset')
            return None

    return lambda_handler
