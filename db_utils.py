import time
import psycopg2

from constants import DAY_LEN
from db_tables import daily_statistics
from db_tables.daily_statistics import DailyStatistic


def collect_data_for_timestamp(database_url, timestamp) -> DailyStatistic:
    with psycopg2.connect(database_url) as connection, connection.cursor() as cursor:
        from_timestamp, to_timestamp = timestamp, timestamp + DAY_LEN

        return DailyStatistic(
            from_timestamp,
            perform_query(cursor, 'transactions_count', from_timestamp, to_timestamp),
            perform_query(cursor, 'teragas_used', from_timestamp, to_timestamp),
            perform_query(cursor, 'deposit_amount', from_timestamp, to_timestamp),
            perform_query(cursor, 'new_accounts_count', from_timestamp, to_timestamp),
            perform_query(cursor, 'deleted_accounts_count', from_timestamp, to_timestamp),
            perform_query(cursor, 'active_accounts_count', from_timestamp, to_timestamp),
            perform_query(cursor, 'new_contracts_count', from_timestamp, to_timestamp),
            perform_query(cursor, 'new_unique_contracts_count', from_timestamp, to_timestamp),
            perform_query(cursor, 'active_contracts_count', from_timestamp, to_timestamp),
        )


def save_statistics(database_url, statistic: DailyStatistic):
    with psycopg2.connect(database_url) as connection, connection.cursor() as cursor:
        cursor.execute(statistic.get_insert_string())


def perform_query(cursor, field, from_timestamp, to_timestamp) -> int:
    aligned_field = f'{field}...'.ljust(30)
    print(f'Computing {aligned_field}', end=' ')
    start_time = time.time()
    query = getattr(daily_statistics, field)(from_timestamp, to_timestamp)
    cursor.execute(query)
    result = cursor.fetchone()[0] or 0
    print(f'Done in {round(time.time() - start_time, 2)} seconds. Result: {result}')
    return result
