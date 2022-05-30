import json
import sys
from typing import TypedDict
import requests.exceptions
import urllib3
from requests import request
from _globals import *
from _logging import log

BAN_ON_CHANGES = '.prod.'


class DBInfo(TypedDict):
    """Class for keeping of a Database info"""
    namespace: str
    microservice: str
    dbtype: str
    classifier: dict
    database: str
    username: str


class DBaaSAggregator:
    """Class DBaaSAggregator provides access to DBaaS aggregator service API"""

    def __init__(self, name: str = ..., url: str = ..., auth: str | None = None, credentials: str | None = None, **kwargs) -> None:
        self._name = name
        self._url = url
        self._auth = auth
        self._credentials = credentials

    @property
    def url(self):
        return self._url

    @staticmethod
    def _get_db_info(dbaas_record: dict) -> DBInfo:
        """Gets database info from DBaaS database record"""
        db_info: DBInfo = {
            'namespace': dbaas_record['namespace'],
            'microservice': dbaas_record['classifier']['microserviceName'],
            'dbtype': dbaas_record['type'],
            'classifier': dbaas_record['classifier'],
            'database': dbaas_record['name'],
            'username': dbaas_record['connectionProperties'].get('username', '')
        }
        return db_info

    def _request(self, method: str, url: str, data: str | None = None) -> requests.Response:
        """Requests to DBaaS API

        :return: Response <Response> object
        :rtype: requests.Response
        """
        # Sets common request parameters
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        if self._auth == 'basic':
            headers['Authorization'] = f"Basic {self._credentials}"
        # if not data:
        #     data = ''
        log.debug(f"Request: method='{method}', url='{url}', headers='{headers}', data='{data}'")
        try:
            # Disables TLS warnings (urllib3.exceptions.InsecureRequestWarning)
            urllib3.disable_warnings()
            # Requests the API
            response = request(method=method, url=url, headers=headers, data=data, verify=False)
        except Exception as err:
            log.critical(repr(err))
            sys.exit()
        log.debug(f"response code: '{response.status_code}', reason: '{response.reason}', data: '{response.text}'")
        return response

    def database_list(self, namespace: str) -> list:
        method = 'GET'
        endpoint = f'{self.url}/api/v1/dbaas/{namespace}/databases/list'
        valid_response_codes = (200,)
        response = self._request(method, endpoint)
        databases = []
        if response.status_code in valid_response_codes:
            dbaas_records = response.json()
            for record in dbaas_records:
                try:
                    databases.append(self._get_db_info(record))
                except KeyError as err:
                    log.warning(f"Missing some data in DBaaS record: {repr(err)}")
                    log.debug(f"DBaaS record: {record}")
        else:
            log.error(f"API call failed: status_code: '{response.status_code}', reason: '{response.reason}', text: '{response.text}'")

        return databases

    def get_by_classifier(self, namespace: str, dbtype: str, classifier: dict) -> DBInfo | None:
        """Calls 'get-by-classifier' API and returns DBaaSAggregator.DatabaseInfo object"""
        method = 'POST'
        endpoint = f'{self.url}/api/v1/dbaas/{namespace}/databases/get-by-classifier/{dbtype}'
        data = json.dumps(classifier)
        log.debug(f"Call: method='{method}', endpoint='{endpoint}', classifier='{classifier}'")
        valid_response_codes = (200,)
        response = self._request(method, endpoint, data)
        if response.status_code in valid_response_codes:
            try:
                database = self._get_db_info(response.json())
            except KeyError as err:
                log.error(f"Missing some data in DBaaS record: {repr(err)}")
                return None
            return database
        else:
            log.error(f"API call failed: status_code: '{response.status_code}', reason: '{response.reason}', text: '{response.text}'")
            return None

    def password_changes(self, classifier: dict, namespace: str, dbtype: str) -> dict:
        """Resets password for database

        :return: dict: {"changed": [ { "classifier": { ... }, "connection": { ... } }, ... ],
                        "failed": [ { "classifier": { ... }, "message": "500 Internal Server Error: ..." }, ... ]}
        """
        if ban_on_changes(self.url, namespace):
            log.critical(f"Any changes are prohibited on the instance.")
            log.debug(f"url: {self.url}; namespace: {namespace}.")
            sys.exit()
        # data = {
        #     'type': database_type,
        #     'classifier': classifier
        # }
        # if (resp := self._request_api('password-changes', namespace, database_type, json.dumps(data))):
        #     resp['changed'][0]['connection']['password'] = '*hidden*'
        #     return resp
        # else:
        #     return None

    def reset_passwords(self, databases: list):
        """Resets passwords for database list

        :return: dict: {
                            "changed": [ { "classifier": { ... }, "connection": { ... } }, ... ],
                            "failed": [ { "classifier": { ... }, "message": "500 Internal Server Error: ..." }, ... ]
                       }
        """
        for db in databases:
            self.password_changes(db)
        ...
