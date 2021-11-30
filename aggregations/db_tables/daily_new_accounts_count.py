from . import DAY_LEN_SECONDS, daily_start_of_range
from ..periodic_aggregations import PeriodicAggregations


class DailyNewAccountsCount(PeriodicAggregations):
    @property
    def sql_create_table(self):
        # Suppose we have at most 10^4 (10K) new accounts per second.
        # It gives ~10^9 new accounts per day.
        # It means we fit into integer (10^9)
        return """
            CREATE TABLE IF NOT EXISTS daily_new_accounts_count
            (
                collected_for_day DATE PRIMARY KEY,
                new_accounts_count INTEGER NOT NULL
            )
        """

    @property
    def sql_drop_table(self):
        return """
            DROP TABLE IF EXISTS daily_new_accounts_count
        """

    @property
    def sql_select(self):
        return """
            SELECT COUNT(created_by_receipt_id)
            FROM accounts
            JOIN receipts ON receipts.receipt_id = accounts.created_by_receipt_id
            WHERE receipts.included_in_block_timestamp >= %(from_timestamp)s
                AND receipts.included_in_block_timestamp < %(to_timestamp)s
        """

    @property
    def sql_insert(self):
        return """
            INSERT INTO daily_new_accounts_count VALUES %s
            ON CONFLICT DO NOTHING
        """

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, timestamp: int) -> int:
        return daily_start_of_range(timestamp)
