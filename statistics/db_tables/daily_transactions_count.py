import typing

from . import DAY_LEN_SECONDS, daily_start_of_range
from ..sql_statistics import SqlStatistics


class DailyTransactionsCount(SqlStatistics):
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
    def sql_select(self):
        return '''
            SELECT COUNT(*) FROM transactions
            WHERE block_timestamp >= %(from_timestamp)s
                AND block_timestamp < %(to_timestamp)s
        '''

    @property
    def sql_insert(self):
        return '''
            INSERT INTO daily_transactions_count VALUES (
                %(time)s,
                %(result)s
            )
        '''

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, requested_statistics_timestamp: typing.Optional[int]) -> int:
        return daily_start_of_range(requested_statistics_timestamp)
