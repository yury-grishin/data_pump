import json
import sys
import requests.exceptions
import urllib3
from requests import request
from _logging import log

EMBARGO_ON_IMPORTS = '.prod.'


class PostgresBackupDaemon:
    """Class PostgresBackupDaemon provides access to postgres-backup-daemon service API"""

    def __init__(self, url: str, auth: str | None = ..., credentials: str | None = ..., **kwargs) -> None:
        self._url = url
        self._auth = auth
        self._credentials = credentials

    @property
    def url(self):
        return self._url

    def _request(self, method: str, url: str, data: str | None = None) -> requests.Response:
        """Requests to postgres-backup-daemon API

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

