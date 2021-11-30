from . import WEEK_LEN_SECONDS, weekly_start_of_range
from ..periodic_aggregations import PeriodicAggregations


class WeeklyActiveAccountsCount(PeriodicAggregations):
    @property
    def sql_create_table(self):
        # For September 2021, we have 10^6 accounts on the Mainnet.
        # It means we fit into integer (10^9)
        return """
            CREATE TABLE IF NOT EXISTS weekly_active_accounts_count
            (
                collected_for_week    DATE PRIMARY KEY, -- start of the week (Monday)
                active_accounts_count INTEGER NOT NULL
            )
        """

    @property
    def sql_drop_table(self):
        return """
            DROP TABLE IF EXISTS weekly_active_accounts_count
        """

    @property
    def sql_select(self):
        return """
            SELECT COUNT(DISTINCT transactions.signer_account_id)
            FROM transactions
            WHERE transactions.block_timestamp >= %(from_timestamp)s
                AND transactions.block_timestamp < %(to_timestamp)s
        """

    @property
    def sql_insert(self):
        return """
            INSERT INTO weekly_active_accounts_count VALUES %s
            ON CONFLICT DO NOTHING
        """

    @property
    def duration_seconds(self):
        return WEEK_LEN_SECONDS

    def start_of_range(self, timestamp: int) -> int:
        return weekly_start_of_range(timestamp)
