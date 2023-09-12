'''The base implementation of SWODLR utility classes'''
from abc import ABC, abstractmethod
import json
import logging
from importlib import resources
from os import getenv
from tempfile import NamedTemporaryFile

import boto3
import fastjsonschema
from requests import Session

import podaac.swodlr_common


class BaseUtilities(ABC):
    '''
    An abstract base utilities class that microservices should extend for their
    own specific requirements
    '''

    def __init__(self, app_name, service_name):
        self._env = getenv('SWODLR_ENV', 'prod')
        self._app_name = app_name
        self._service_name = service_name
        self._ssm_path = f'/service/{self._app_name}/{self._service_name}/'

        if self._env == 'prod':
            self._load_params_from_ssm()
        else:
            from dotenv import load_dotenv  # noqa: E501 # pylint: disable=import-outside-toplevel
            load_dotenv()

    @classmethod
    @abstractmethod
    def get_instance(cls):
        '''
        Should be implemented by subclasses to create singleton instances of
        the final utilities class
        '''

    def _load_params_from_ssm(self):
        ssm = boto3.client('ssm')

        parameters = []
        next_token = None
        while True:
            kwargs = {'NextToken': next_token} \
                if next_token is not None else {}
            res = ssm.get_parameters_by_path(
                Path=self._ssm_path,
                WithDecryption=True,
                **kwargs
            )

            parameters.extend(res['Parameters'])
            if 'NextToken' in res:
                next_token = res['NextToken']
            else:
                break

        self._ssm_parameters = {}

        for param in parameters:
            name = param['Name'].removeprefix(self._ssm_path)
            self._ssm_parameters[name] = param['Value']

    def _get_sds_session(self):
        '''
        Lazily create authenticated session for internal use

        CAUTION: THE SESSION OBJECT IS AUTHENTICATED. DO NOT USE THIS SESSION
        OUTSIDE OF THIS UTILITY CLASS OR CREDENTIALS MAY LEAK
        '''
        if not hasattr(self, '_session'):
            ca_cert = self.get_param('sds_ca_cert')
            username = self.get_param('sds_username')
            password = self.get_param('sds_password')

            session = Session()
            session.auth = (username, password)

            if ca_cert is not None:
                # pylint: disable=consider-using-with
                cert_file = NamedTemporaryFile('w', delete=False)
                cert_file.write(ca_cert)
                cert_file.flush()
                session.verify = cert_file.name

            self._session = session  # noqa: E501 # pylint: disable=attribute-defined-outside-init

        return self._session

    def get_param(self, name):
        '''
        Retrieves a parameter from SSM or the environment depending on the
        environment
        '''
        if self._env == 'prod':
            return self._ssm_parameters.get(name)

        return getenv(f'{self._app_name.upper()}_{name}')

    def get_logger(self, name):
        '''
        Creates a logger for a requestor with a global log level defined from
        parameters
        '''
        logger = logging.getLogger(name)

        log_level = getattr(logging, self.get_param('log_level')) \
            if self.get_param('log_level') is not None else logging.INFO
        logger.setLevel(log_level)
        return logger

    def load_json_schema(self, name):
        '''
        Load a json schema from the schema folder and return the compiled
        schema
        '''
        schemas = resources.files(podaac.swodlr_common) \
            .joinpath('schemas')
        schema_resource = schemas.joinpath(f'{name}.json')

        if not schema_resource.is_file():
            raise RuntimeError('Schema not found')

        with schema_resource.open('r', encoding='utf-8') as schema_json:
            return fastjsonschema.compile(json.load(schema_json))
