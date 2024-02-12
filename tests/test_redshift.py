import pytest

from bluescope.db.redshift import RedshiftServerlessConnection  # Adjust the import path as necessary


@pytest.fixture
def mock_rs_serverless_connection(mocker, mock_redshift_conn, db_params):
    """Mock the RedshiftServerlessConnection class."""
    mock_connection, mock_cursor = mock_redshift_conn
    mock_cursor.fetchone.return_value = tuple([db_params['user_name'], db_params['user_id']])
    conn = RedshiftServerlessConnection(host=db_params['host'],
                                        port=db_params['port'],
                                        db=db_params['db'],
                                        user=db_params['user'],
                                        password=db_params['password'])
    return conn, mock_connection, mock_cursor


def test_conn_variables(mock_rs_serverless_connection, db_params):
    connection = mock_rs_serverless_connection[0]
    assert connection.user_name == db_params['user_name']
    assert connection.user_id == db_params['user_id']
    assert connection.user == db_params['user']
    assert connection.host == db_params['host']
    assert connection.port == db_params['port']
    assert connection.db == db_params['db']
    assert connection.password == db_params['password']


def test_get_stats(mock_rs_serverless_connection, mock_time_sleep):
    conn, mock_connection, mock_cursor = mock_rs_serverless_connection

    mock_cursor.fetchone.return_value = tuple([12345, 10.5])  # Mocked query_id and execution_time

    # Call the method under test
    query_id, execution_time = conn.get_stats('SELECT 1', '2023-01-01 00:00:00.00')

    # Assertions to validate behavior
    assert query_id == 12345
    assert execution_time == 10.5
    mock_cursor.execute.assert_called()  # Assert that execute was called
    # You can add more assertions here to validate the execute call parameters if necessary


def test_execute(mock_rs_serverless_connection):
    conn, mock_connection, mock_cursor = mock_rs_serverless_connection

    mock_cursor.rowcount = 1  # Mocked row count
    # Call the method under test
    row_count = conn.execute('SELECT 1', {})

    # Assertions to validate behavior
    mock_cursor.execute.assert_called_with('SELECT 1', {})
    assert row_count == 1
    mock_cursor.execute.assert_called()  # Assert that execute was called
    # You can add more assertions here to validate the execute call parameters if necessary
