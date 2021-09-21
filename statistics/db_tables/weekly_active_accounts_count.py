import typing

from . import WEEK_LEN_SECONDS, weekly_start_of_range
from ..periodic_statistics import PeriodicStatistics


class WeeklyActiveAccountsCount(PeriodicStatistics):
    @property
    def sql_create_table(self):
        # For September 2021, we have 10^6 accounts on the Mainnet.
        # It means we fit into integer (10^9)
        return '''
            CREATE TABLE IF NOT EXISTS weekly_active_accounts_count
            (
                collected_for_week    DATE PRIMARY KEY, -- start of the week (Monday)
                active_accounts_count INTEGER NOT NULL
            )
        '''

    @property
    def sql_drop_table(self):
        return '''
            DROP TABLE IF EXISTS weekly_active_accounts_count
        '''

    @property
    def sql_select(self):
        return '''
            SELECT COUNT(DISTINCT transactions.signer_account_id)
            FROM transactions
            JOIN execution_outcomes ON execution_outcomes.receipt_id = transactions.converted_into_receipt_id
            WHERE transactions.block_timestamp >= %(from_timestamp)s
                AND transactions.block_timestamp < %(to_timestamp)s
                AND execution_outcomes.status IN ('SUCCESS_VALUE', 'SUCCESS_RECEIPT_ID')
        '''

    @property
    def sql_select_all(self):
        return '''
            SELECT
                DATE_TRUNC('week', TO_TIMESTAMP(DIV(transactions.block_timestamp, 1000 * 1000 * 1000))) AS date,
                COUNT(DISTINCT transactions.signer_account_id) AS active_accounts_count_by_week
            FROM transactions
            JOIN execution_outcomes ON execution_outcomes.receipt_id = transactions.converted_into_receipt_id
            WHERE execution_outcomes.status IN ('SUCCESS_VALUE', 'SUCCESS_RECEIPT_ID')
                AND transactions.block_timestamp < ((CAST(EXTRACT(EPOCH FROM DATE_TRUNC('week', NOW())) AS bigint)) * 1000 * 1000 * 1000)
            GROUP BY date
            ORDER BY date
        '''

    @property
    def sql_insert(self):
        return '''
            INSERT INTO weekly_active_accounts_count VALUES %s
            ON CONFLICT DO NOTHING
        '''

    @property
    def duration_seconds(self):
        return WEEK_LEN_SECONDS

    def start_of_range(self, requested_statistics_timestamp: typing.Optional[int]) -> int:
        return weekly_start_of_range(requested_statistics_timestamp)
