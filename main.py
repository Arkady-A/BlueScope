import argparse

from bluescope.statsutils import find_significance
from bluescope.logger import setup_logger
from bluescope import profiler_classes, get_profiler

logger = setup_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Run database profiler based on the provided arguments.')

    # define expected arguments
    parser.add_argument('--db_type', type=str, required=True, choices=profiler_classes.keys(),
                        help='Type of the database')
    parser.add_argument('--host', type=str, required=True, help='Database host')
    parser.add_argument('--port', type=int, default=5439, help='Database port')
    parser.add_argument('--db', type=str, required=True, help='Database name')
    parser.add_argument('--user', type=str, required=True, help='Database user')
    parser.add_argument('--password', type=str, default='', help='Database password')
    # flag to agree given sample size
    parser.add_argument('--agree', action='store_true', help='Agree to the sample size')

    # add arguments for SQL query strings or file paths
    parser.add_argument('--sql_query_1', type=str, required=True, help='SQL query 1 as a string or a file path')
    parser.add_argument('--sql_query_2', type=str, required=False, help='SQL query 2 as a string or a file path')

    args = parser.parse_args()

    # determine if arguments are file paths or SQL queries directly
    if args.sql_query_1 and args.sql_query_1.endswith('.sql'):
        with open(args.sql_query_1, 'r') as file:
            sql_query_1 = file.read()
    else:
        sql_query_1 = args.sql_query_1

    if args.sql_query_2 and args.sql_query_2.endswith('.sql'):
        with open(args.sql_query_2, 'r') as file:
            sql_query_2 = file.read()
    else:
        sql_query_2 = args.sql_query_2

    profiler = get_profiler(args['db_type'])(host=args.host,
                                             port=args.port,
                                             db=args.db,
                                             user=args.user,
                                             password=args.password,
                                             agree=args.agree)
    sql_query_1_profile = profiler.profile(sql_query_1, {})
    if sql_query_2:
        sql_query_2_profile = profiler.profile(sql_query_2, {})
        # compare the profiles
        p = find_significance(sql_query_1_profile['mean'], sql_query_2_profile['mean'], sql_query_1_profile['std'],
                              sql_query_2_profile['std'], sql_query_1_profile['sample_size'],
                              sql_query_2_profile['sample_size'])
        logger.info(f"p-value: {p}")


if __name__ == "__main__":
    main()
