'''
Decorators utilized to declare job handlers and inject lambda_handlers
within service modules
'''
from copy import deepcopy
import inspect
import sys
from time import sleep
import traceback
from weakref import WeakSet

from fastjsonschema import JsonSchemaException

from .logging import JobMetadataInjector
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
        _generate_bulk_job_handler(handler, module)
    )
    return handler


def bulk_job_handler(*args, **kwargs):
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

    def inner(handler):
        module = inspect.getmodule(handler)
        if module not in loaded_modules:
            loaded_modules.add(module)
        else:
            raise RuntimeError(
                'Multiple job handlers cannot be declared in one module'
            )

        # Inject our lambda_handler into the module
        module.lambda_handler = _generate_lambda_handler(
            handler, returns_jobset
        )
        return handler

    returns_jobset = False

    if len(args) == 1:
        if callable(args[0]):
            returns_jobset = False
            return inner(args[0])
        if isinstance(args[0], bool):
            returns_jobset = args[0]
            return inner
    elif 'returns_jobset' in kwargs:
        returns_jobset = kwargs['returns_jobset']
        return inner

    raise RuntimeError('Invalid args provided')


def _generate_bulk_job_handler(handler, module):
    utils = BaseUtilities.get_instance()
    logger = utils.get_logger(__name__)
    module_logger = utils.get_logger(module.__name__)
    max_attempts = int(utils.get_param('max_attempts')) \
        if utils.get_param('max_attempts') is not None else 3
    pass_job_logger = len(inspect.signature(handler).parameters) >= 2

    def try_handler(input_job):
        if pass_job_logger:
            # Add injector to provide job metadata
            job_logger = JobMetadataInjector(module_logger, input_job)

        for duration in [i**2 for i in range(0, max_attempts + 1)]:
            if duration > 0:
                logger.info('Backing off for %d seconds', duration)
                sleep(duration)
            try:
                input_job_copy = deepcopy(input_job)
                input_params = (input_job_copy, job_logger) \
                    if pass_job_logger else (input_job_copy,)

                return handler(*input_params)
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
        try:
            input_jobset = validate_jobset(event)
        except JsonSchemaException:
            logger.exception('Error validating input jobset')
            logger.error(event)

        input_jobs = input_jobset['jobs']
        logger.info('Received %d jobs', len(input_jobs))

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
