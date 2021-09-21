import argparse
import dotenv
import os
import psycopg2
import time

from statistics import DailyActiveAccountsCount, DailyActiveContractsCount, DailyDeletedAccountsCount, \
    DailyDepositAmount, DailyGasUsed, DailyNewAccountsCount, DailyNewContractsCount, DailyNewUniqueContractsCount, \
    DailyTransactionsCount, DeployedContracts, WeeklyActiveAccountsCount

STATS = {
    'daily_active_accounts_count': DailyActiveAccountsCount,
    'daily_active_contracts_count': DailyActiveContractsCount,
    'daily_deleted_accounts_count': DailyDeletedAccountsCount,
    'daily_deposit_amount': DailyDepositAmount,
    'daily_gas_used': DailyGasUsed,
    'daily_new_accounts_count': DailyNewAccountsCount,
    'daily_new_contracts_count': DailyNewContractsCount,
    'daily_new_unique_contracts_count': DailyNewUniqueContractsCount,
    'daily_transactions_count': DailyTransactionsCount,
    'deployed_contracts': DeployedContracts,
    'weekly_active_accounts_count': WeeklyActiveAccountsCount,
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
    parser.add_argument('-a', '--all', action='store_true',
                        help='Drop all previous data for given `stats-types` and fulfill the DB '
                             'with all values till now. Timestamp parameter will be ignored.')
    args = parser.parse_args()

    dotenv.load_dotenv()
    ANALYTICS_DATABASE_URL = os.getenv('ANALYTICS_DATABASE_URL')
    INDEXER_DATABASE_URL = os.getenv('INDEXER_DATABASE_URL')

    with psycopg2.connect(ANALYTICS_DATABASE_URL) as analytics_connection, \
            psycopg2.connect(INDEXER_DATABASE_URL) as indexer_connection:
        # TODO check if the DB is ready to collect data for this date
        # TODO make dependencies for stats (daily_new_unique_contracts_count depends on deployed_contracts)
        for statistics_type in args.stats_types or STATS:
            try:
                aligned_field = f'{statistics_type}...'.ljust(35)
                print(f'Computing {aligned_field}', end=' ')
                start_time = time.time()

                statistics_cls = STATS[statistics_type]
                statistics = statistics_cls(analytics_connection, indexer_connection)
                statistics.create_table(drop_previous_table=args.all)
                result = statistics.collect_statistics(args.timestamp, args.all)
                statistics.store_statistics(result)

                print(f'Done in {round(time.time() - start_time, 1)} seconds. Result: {result}')
            except Exception as e:
                # Let's at least try to collect other stats
                analytics_connection.rollback()
                indexer_connection.rollback()
                print(e)
