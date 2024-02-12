import pytest

from bluescope.db.redshift import RedshiftServerlessConnection


@pytest.fixture(scope='module')
def db_params():
    return {
        'host': 'localhost',
        'port': 5439,
        'db': 'mydb',
        'user': 'user',
        'password': 'password',
        'user_id': '1234567890',
        'user_name': 'mocked_user'
    }


@pytest.fixture(scope='module')
def mock_time_sleep(module_mocker):
    """Mock the sleep function from the time module."""
    module_mocker.patch('time.sleep', return_value=None)


@pytest.fixture(scope='module')
def mock_redshift_conn(module_mocker):
    """Mock the Redshift connection and cursor."""
    mock_cursor = module_mocker.MagicMock()
    mock_cursor.fetchone.return_value = tuple(['mocked_user', '1234567890'])
    mock_connection = module_mocker.MagicMock()
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    module_mocker.patch('redshift_connector.connect', return_value=mock_connection)
    return mock_connection, mock_cursor


@pytest.fixture(scope='module')
def mock_rs_serverless_connection(mock_redshift_conn, db_params):
    """Mock the RedshiftServerlessConnection class."""
    mock_connection, mock_cursor = mock_redshift_conn
    mock_cursor.fetchone.return_value = tuple([db_params['user_name'], db_params['user_id']])
    conn = RedshiftServerlessConnection(host=db_params['host'],
                                        port=db_params['port'],
                                        db=db_params['db'],
                                        user=db_params['user'],
                                        password=db_params['password'])
    return conn, mock_connection, mock_cursor

# @pytest.fixture(scope='module')
# def mock_fetchone_user():
#
