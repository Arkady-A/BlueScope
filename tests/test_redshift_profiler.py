from bluescope import RedshiftServerlessProfiler


def test_profile(mocker, db_params, mock_redshift_conn, mock_time_sleep):
    profiler = RedshiftServerlessProfiler(
        host=db_params['host'],
        port=db_params['port'],
        db=db_params['db'],
        user=db_params['user'],
        password=db_params['password'],
        agree=True
    )
    get_stats_mocker = mocker.MagicMock()
    get_stats_mocker.return_value = (12345, 10.5)
    mocker.patch('bluescope.db.redshift.RedshiftServerlessConnection.get_stats', get_stats_mocker)

    mean, std, sample_size = profiler.profile('SELECT 1', {})
    assert mean == 10.5
    assert std == 0
    assert profiler.connection.get_stats.called


