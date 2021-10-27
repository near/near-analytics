import datetime

from . import DAY_LEN_SECONDS, daily_start_of_range
from ..periodic_aggregations import PeriodicAggregations

class DailyNewAccountsPerPredecessorAccountCount(PeriodicAggregations):
    @property
    def sql_create_table(self):
        # Suppose we have at most 10^4 (10K) new accounts per second.
        # It gives ~10^9 new accounts per day.
        # It means we fit into integer (10^9)
        return '''

            CREATE TABLE IF NOT EXISTS daily_new_accounts_per_predecessor_account_count
            (
                collected_for_day DATE NOT NULL,
                predecessor_account_id TEXT NOT NULL,
                new_accounts_count BIGINT NOT NULL,
                PRIMARY KEY (collected_for_day, predecessor_account_id)
            );
            CREATE INDEX IF NOT EXISTS daily_new_accounts_per_predecessor_account_count_idx
                ON daily_new_accounts_per_predecessor_account_count (collected_for_day, new_accounts_count DESC)
        '''

    @property
    def sql_drop_table(self):
        return '''
            DROP TABLE IF EXISTS daily_new_accounts_per_predecessor_account_count
        '''

    @property
    def sql_select(self):
        return '''
            SELECT 
            receipts.predecessor_account_id,
            COUNT(*) AS new_accounts_count
            FROM accounts
            JOIN receipts ON receipts.receipt_id = accounts.created_by_receipt_id
            WHERE receipts.included_in_block_timestamp >= %(from_timestamp)s
                AND receipts.included_in_block_timestamp < %(to_timestamp)s
            GROUP BY receipts.predecessor_account_id    
        '''

    @property
    def sql_insert(self):
        return '''
            INSERT INTO daily_new_accounts_per_predecessor_account_count VALUES %s
            ON CONFLICT DO NOTHING
        '''

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, timestamp: int) -> int:
        return daily_start_of_range(timestamp)

    @staticmethod
    def prepare_data(parameters: list, **kwargs) -> list:
        computed_for = datetime.datetime.utcfromtimestamp(kwargs['start_of_range']).strftime('%Y-%m-%d')
        return [(computed_for, predecessor_account_id, new_accounts_count) for (predecessor_account_id, new_accounts_count) in parameters]