import argparse
import dotenv
import os
import psycopg2
import time

from statistics import DailyGasUsed, DailyTransactionsCount

STATS = {
    'daily_transactions_count': DailyTransactionsCount,
    'daily_gas_used': DailyGasUsed,
}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compute statistics for mainnet')
    parser.add_argument('-t', '--timestamp', type=int,
                        help='The timestamp in seconds precision, indicates the period for computing the statistics. '
                             'The rounding will be performed. '
                             'By default, takes the period before the current '
                             '(yesterday, previous week starting Monday, previous calendar month).')
    parser.add_argument('-s', '--stats-types', nargs='+', choices=STATS, default=[],
                        help='The type of statistics to compute. By default, everything will be computed.')
    args = parser.parse_args()

    dotenv.load_dotenv()
    ANALYTICS_DATABASE_URL = os.getenv('ANALYTICS_DATABASE_URL')
    INDEXER_DATABASE_URL = os.getenv('INDEXER_DATABASE_URL')

    with psycopg2.connect(ANALYTICS_DATABASE_URL) as analytics_connection, \
            psycopg2.connect(INDEXER_DATABASE_URL) as indexer_connection:
        for statistics_type in args.stats_types or STATS:
            try:
                aligned_field = f'{statistics_type}...'.ljust(30)
                print(f'Computing {aligned_field}', end=' ')
                start_time = time.time()

                statistics_cls = STATS[statistics_type]
                statistics = statistics_cls(analytics_connection, indexer_connection)
                statistics.create_tables()
                result = statistics.collect_statistics(args.timestamp)
                statistics.store_statistics(result)

                print(f'Done in {round(time.time() - start_time, 2)} seconds. Result: {result}')
            except Exception as e:
                # Let's at least try to collect other stats
                print(e)
