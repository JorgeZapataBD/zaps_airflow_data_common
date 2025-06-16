from typing import Union
from urllib.parse import urljoin

import requests
from airflow.hooks.base import BaseHook


class ApiCustomHook(BaseHook):
    """
    Interact with any API.

    This hook uses HTTP Connection.

    :param conn_id: The Airflow connection used for API credentials.
    :param auth: request Authentication Object for api credentials.
    """

    def __init__(
        self,
        conn_id: str,
        auth: requests.auth.AuthBase = None
    ) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.auth = auth
        self.connection = self.get_connection(conn_id)
        self.headers = self.connection.extra_dejson.get('headers')

    def get_headers(self) -> dict | None:
        """
        Return headers from connection extra parameters
        :return dict: Requests headers
        """
        return self.connection.extra_dejson.get('headers')

    def get_conn(self) -> requests.Session:
        """
        Create Request Session.
        :return session: request Session
        """
        session = requests.Session()
        if self.auth:
            session.auth = self.auth
        return session

    def get_data(
        self,
        endpoint: str | None = None,
        headers: dict | None = None,
        stream: bool = False,
        timeout: int = 60,
        **params
    ) -> Union[dict, list]:
        """
        Get data from API Endpoint.

        This hook uses HTTP Connection.

        :param conn_id: The Airflow connection used for API credentials.
        :param auth: request Authentication Object for api credentials.
        """
        uri = urljoin(self.connection.host.rstrip('/'), endpoint)
        session = self.get_conn()
        self.log.info(f'Connectiong to {uri}')
        try:
            response = session.get(
                uri,
                headers=headers or self.headers,
                timeout=timeout,
                params=params,
                stream=stream
            )
            if response.status_code < 200 or response.status_code > 399:
                self.log.error(f'Request Code Error: {response.status_code}')
                response.raise_for_status()
            self.log.info(
                f'Request to {response.url} successful and data retrieved')
            return response
        except requests.exceptions.HTTPError as http_err:
            self.log.error(f'HTTP Error: {http_err}')
            raise
        except requests.exceptions.ConnectionError as conn_err:
            self.log.error(f'Conenction Error: {conn_err}')
            raise
        except requests.exceptions.Timeout as timeout_err:
            self.log.error(f'TimeOut Error: {timeout_err}')
            raise
        except requests.exceptions.RequestException as req_err:
            self.log.error(f'RequestException Error: {req_err}')
            raise
