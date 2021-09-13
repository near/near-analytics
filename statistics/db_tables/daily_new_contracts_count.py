import typing

from . import DAY_LEN_SECONDS, daily_start_of_range
from ..sql_statistics import SqlStatistics


class DailyNewContractsCount(SqlStatistics):
    @property
    def sql_create_table(self):
        # Suppose we have at most 10^4 (10K) new contracts per second.
        # It gives ~10^9 new contracts per day.
        # It means we fit into integer (10^9)
        return '''
            CREATE TABLE IF NOT EXISTS daily_new_contracts_count
            (
                collected_for_day   DATE PRIMARY KEY,
                new_contracts_count INTEGER NOT NULL
            )
        '''

    @property
    def sql_select(self):
        return '''
            SELECT COUNT(DISTINCT receipts.receiver_account_id)
            FROM action_receipt_actions
            JOIN receipts ON receipts.receipt_id = action_receipt_actions.receipt_id
            WHERE receipts.included_in_block_timestamp > %(from_timestamp)s
                AND receipts.included_in_block_timestamp < %(to_timestamp)s
                AND action_receipt_actions.action_kind = 'DEPLOY_CONTRACT'
        '''

    @property
    def sql_insert(self):
        return '''
            INSERT INTO daily_new_contracts_count VALUES (
                %(collected_for_day)s,
                %(result)s
            )
        '''

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, requested_statistics_timestamp: typing.Optional[int]) -> int:
        return daily_start_of_range(requested_statistics_timestamp)
