import pytest
from airflow.models.connection import Connection

from zaps_provider.hooks.api_custom import ApiCustomHook


def test_apicustomhook_connection_parameters(mock_connection):
    hook = ApiCustomHook(conn_id="api_conn_id")
    assert hook.headers == {'Accept': 'test'}
    assert hook.connection.host == "https://api.esios.ree.es"


@pytest.fixture
def mock_connection(mocker):
    mock_connection = Connection(
        conn_type="http",
        host="https://api.esios.ree.es",
        extra={'headers': {'Accept': 'test'}}
    )
    mock_connection_uri = mock_connection.get_uri()
    mocker.patch.dict(
        "os.environ", AIRFLOW_CONN_API_CONN_ID=mock_connection_uri)
