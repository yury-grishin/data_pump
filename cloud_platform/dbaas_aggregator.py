"""
DBaaS Aggregator module
"""
import logging
from dataclasses import dataclass
import requests.exceptions
import urllib3
from requests import request
from _globals import *
from _logging import log


class DBaaSAggregator:
    """Class DBaaSAggregator provides access to DBaaS aggregator service API"""

    @dataclass
    class DBaaSItem:
        """Dataclass for keeping of a DBaaS item

        Keys args:
            namespace:
            microservice:
            dbtype:
            classifier:

        """
        microservice: str
        namespace: str
        type: str
        classifier: dict

        def __init__(self, namespace: str = None, dbtype: str = None, classifier: dict = None, **kwargs) -> None:
            self.namespace = namespace
            self.microservice = classifier['microserviceName']
            self.dbtype = dbtype
            self.classifier = classifier

    def __init__(self, url: str, auth: str | None, credentials: str | None) -> None:
        self._url = url
        self._auth = auth
        self._credentials = credentials
        self._embargo = []

    def _request(self, method: str, url: str, data: str = '') -> requests.Response:
        """Requests to DBaaS API

        :return: Response <Response> object
        :rtype: requests.Response
        """
        # Sets common request parameters
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        if self._auth == 'basic' and self.is_base64(self._credentials):
            headers['Authorization'] = f"Basic {self._credentials}"
        logger.debug(f"Request: method='{method}', url='{url}', headers='{headers}', data='{data}'")
        try:
            # Disables TLS warnings (urllib3.exceptions.InsecureRequestWarning)
            urllib3.disable_warnings()
            # Requests the API
            response = request(method=method, url=url, headers=headers, data=data, verify=False)
        except Exception as err:
            logger.error(repr(err))
            sys.exit()
        logger.debug(f"response code: '{response.status_code}', reason: '{response.reason}', data: '{response.text}'")
        return response

    @property
    def url(self):
        log.info("Info message.")
        log.warning("Warning message.")
        log.error("Error message.")
        log.critical("Critical message.")

        return self._url
