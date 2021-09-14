import typing

from . import DAY_LEN_SECONDS, daily_start_of_range
from ..sql_statistics import SqlStatistics


class DailyActiveAccountsCount(SqlStatistics):
    @property
    def sql_create_table(self):
        # For September 2021, we have 10^6 accounts on the Mainnet.
        # It means we fit into integer (10^9)
        return '''
            CREATE TABLE IF NOT EXISTS daily_active_accounts_count
            (
                collected_for_day     DATE PRIMARY KEY,
                active_accounts_count INTEGER NOT NULL
            )
        '''

    @property
    def sql_select(self):
        return '''
            SELECT COUNT(DISTINCT transactions.signer_account_id)
            FROM transactions
            JOIN execution_outcomes ON execution_outcomes.receipt_id = transactions.converted_into_receipt_id
            WHERE transactions.block_timestamp > %(from_timestamp)s
                AND transactions.block_timestamp < %(to_timestamp)s
                AND execution_outcomes.status IN ('SUCCESS_VALUE', 'SUCCESS_RECEIPT_ID')
        '''

    @property
    def sql_insert(self):
        return '''
            INSERT INTO daily_active_accounts_count VALUES (
                %(time)s,
                %(result)s
            )
        '''

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, requested_statistics_timestamp: typing.Optional[int]) -> int:
        return daily_start_of_range(requested_statistics_timestamp)
