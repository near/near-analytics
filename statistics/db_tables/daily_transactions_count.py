import typing

from . import DAY_LEN_SECONDS, daily_start_of_range
from ..periodic_statistics import PeriodicStatistics


class DailyTransactionsCount(PeriodicStatistics):
    @property
    def sql_create_table(self):
        # Suppose we have at most 10^5 (100K) transactions per second.
        # It gives ~10^10 transactions per day.
        # It means we fit into BIGINT (10^18)
        return '''
            CREATE TABLE IF NOT EXISTS daily_transactions_count
            (
                collected_for_day  DATE PRIMARY KEY,
                transactions_count BIGINT NOT NULL
            )
        '''

    @property
    def sql_drop_table(self):
        return '''
            DROP TABLE IF EXISTS daily_transactions_count
        '''

    @property
    def sql_select(self):
        return '''
            SELECT COUNT(*) FROM transactions
            WHERE block_timestamp >= %(from_timestamp)s
                AND block_timestamp < %(to_timestamp)s
        '''

    @property
    def sql_select_all(self):
        return '''
            SELECT
                DATE_TRUNC('day', TO_TIMESTAMP(DIV(transactions.block_timestamp, 1000 * 1000 * 1000))) AS date,
                COUNT(*) AS transactions_count
            FROM transactions
            WHERE transactions.block_timestamp < (CAST(EXTRACT(EPOCH FROM DATE_TRUNC('day', NOW())) AS bigint) * 1000 * 1000 * 1000)
            GROUP BY date
            ORDER BY date
        '''

    @property
    def sql_insert(self):
        return '''
            INSERT INTO daily_transactions_count VALUES %s
            ON CONFLICT DO NOTHING
        '''

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, requested_statistics_timestamp: typing.Optional[int]) -> int:
        return daily_start_of_range(requested_statistics_timestamp)
