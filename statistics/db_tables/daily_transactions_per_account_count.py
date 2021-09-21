import datetime
import typing

from . import DAY_LEN_SECONDS, daily_start_of_range
from ..periodic_statistics import PeriodicStatistics


class DailyTransactionsPerAccountCount(PeriodicStatistics):
    @property
    def sql_create_table(self):
        # Suppose we have at most 10^5 (100K) transactions per second.
        # In the worst case, they are all from one account.
        # It gives ~10^10 transactions per day.
        # It means we fit into BIGINT (10^18)
        return '''
            CREATE TABLE IF NOT EXISTS daily_transactions_per_account_count
            (
                collected_for_day  DATE NOT NULL,
                account_id         TEXT NOT NULL,
                transactions_count BIGINT NOT NULL,
                CONSTRAINT daily_transactions_per_account_count_pk PRIMARY KEY (collected_for_day, account_id)
            );
            CREATE INDEX IF NOT EXISTS daily_transactions_per_account_count_idx
                ON daily_transactions_per_account_count (collected_for_day, transactions_count DESC)
        '''

    @property
    def sql_drop_table(self):
        return '''
            DROP TABLE IF EXISTS daily_transactions_per_account_count
        '''

    @property
    def sql_select(self):
        # TODO maybe we want to compute only successful transactions?
        return '''
            SELECT
                signer_account_id,
                COUNT(*) AS transactions_count
            FROM transactions
            WHERE transactions.block_timestamp >= %(from_timestamp)s
                AND transactions.block_timestamp < %(to_timestamp)s
            GROUP BY signer_account_id
        '''

    @property
    def sql_select_all(self):
        # It's not a good idea to calculate it through one select because there will be too much values,
        # I am waiting for 8 millions of rows. It could not fit into RAM on most machines, and INSERT will fail.
        # We have to fill it by for-cycle on Python level
        raise NotImplementedError

    @property
    def sql_insert(self):
        return '''
            INSERT INTO daily_transactions_per_account_count VALUES %s
            ON CONFLICT DO NOTHING
        '''

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, requested_statistics_timestamp: typing.Optional[int]) -> int:
        return daily_start_of_range(requested_statistics_timestamp)

    @staticmethod
    def prepare_data(parameters: list, **kwargs) -> list:
        computed_for = datetime.datetime.utcfromtimestamp(kwargs['start_of_range']).strftime('%Y-%m-%d')
        return [(computed_for, account_id, count) for (account_id, count) in parameters]
