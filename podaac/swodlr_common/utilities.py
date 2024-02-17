'''The base implementation of SWODLR utility classes'''
from abc import ABC
import json
import logging
from importlib import resources
from os import getenv, path
import re
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

import boto3
import fastjsonschema
from elasticsearch import Elasticsearch
from requests import Session

import podaac.swodlr_common


class _SemVer:
    VERSION_RE = re.compile(r'(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)')

    @staticmethod
    def attempt_parse(semver_str):
        '''
        Attempts to parse a string into a SemVer object; if no semver-like
        structures are found in the string, the method will return a None
        '''
        parsed_version = _SemVer.VERSION_RE.search(semver_str)
        if parsed_version is None:
            return None

        return _SemVer(
            parsed_version.group('major'),
            parsed_version.group('minor'),
            parsed_version.group('patch')
        )

    def __init__(self, major, minor, patch):
        self.major = int(major)
        self.minor = int(minor)
        self.patch = int(patch)

    def __eq__(self, other) -> bool:
        if not isinstance(other, type(self)):
            return super().__eq__(other)

        return (
            self.major == other.major and
            self.minor == other.minor and
            self.patch == other.patch
        )

    def __lt__(self, other) -> bool:
        if not isinstance(other, type(self)):
            return super().__lt__(other)

        return (
            self.major < other.major or
            self.minor < other.minor or
            self.patch < other.patch
        )

    def __gt__(self, other) -> bool:
        if not isinstance(other, type(self)):
            return super().__gt__(other)

        return (
            self.major > other.major or
            self.minor > other.minor or
            self.patch > other.patch
        )

    def __le__(self, other) -> bool:
        if not isinstance(other, type(self)):
            return super().__le__(other)

        return (self.__lt__(other) or self.__eq__(other))

    def __ge__(self, other) -> bool:
        if not isinstance(other, type(self)):
            return super().__ge__(other)

        return (self.__gt__(other) or self.__eq__(other))


class BaseUtilities(ABC):
    '''
    An abstract base utilities class that microservices should extend for their
    own specific requirements
    '''

    def __init__(self, app_name, service_name):
        if hasattr(BaseUtilities, '_instance'):
            raise RuntimeError('Utilities were already initialized')

        BaseUtilities._instance = self
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
    def get_instance(cls):
        '''
        Returns the already initiated instance of a subclass which extends
        BaseUtilities
        '''
        if not hasattr(cls, '_instance'):
            raise RuntimeError('Utilities were not initialized yet')

        return cls._instance

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
        def load_local(name):
            name = name.removeprefix('swodlr-')
            schemas = resources.files(podaac.swodlr_common) \
                .joinpath('schemas')
            schema_resource = schemas.joinpath(f'{name}.json')

            if not schema_resource.is_file():
                raise RuntimeError('Schema not found')

            with schema_resource.open('r', encoding='utf-8') as schema_json:
                return json.load(schema_json)

        return fastjsonschema.compile(
            definition=load_local(name),
            handlers={'': load_local}
        )
        
    def get_mozart_es_client(self):
        if not hasattr(self, '_mozart_es_client'):
            base_sds_url = urlparse(self.get_param('sds_host'))
            base_path = base_sds_url.path
            mozart_es_path = path.join(base_path, '/mozart_es/')

            # pylint: disable=attribute-defined-outside-init
            self._mozart_es_client = Elasticsearch(
                mozart_es_path,
                basic_auth=(
                    self.get_param('sds_username'),
                    self.get_param('sds_password')
                )
            )
        
        return self._mozart_es_client

    def get_latest_job_version(self, job_name):
        '''
        Retrieves the latest version of a job spec with a (very) lazy version
        parsing and sorting algorithm
        '''
        if self.get_param('sds_pcm_release_tag') is not None:
            return self.get_param('sds_pcm_release_tag')

        mozart_es_client = self.get_mozart_es_client()

        results = mozart_es_client.search(index='job_specs', query={
            'prefix': {
                'id.keyword': {
                    'value': f'${job_name}:'
                }
            }
        })

        if len(results['hits']['hits']) == 0:
            raise RuntimeError('Specified job spec not found')

        job_versions = {}
        for result in results['hits']['hits']:
            version = _SemVer.attempt_parse(result['_source']['job-version'])
            if version is None:
                # No semver compatible version found
                continue

            if version not in job_versions:
                job_versions[version] = result['_source']

        return job_versions[sorted(job_versions.keys(), reverse=True)[0]]
