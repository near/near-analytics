from . import DAY_LEN_SECONDS, daily_start_of_range
from ..periodic_aggregations import PeriodicAggregations


class DailyActiveContractsCount(PeriodicAggregations):
    @property
    def sql_create_table(self):
        # For September 2021, we have 10^6 accounts on the Mainnet.
        # It means we fit into integer (10^9)
        return """
            CREATE TABLE IF NOT EXISTS daily_active_contracts_count
            (
                collected_for_day      DATE PRIMARY KEY,
                active_contracts_count INTEGER NOT NULL
            )
        """

    @property
    def sql_drop_table(self):
        return """
            DROP TABLE IF EXISTS daily_active_contracts_count
        """

    @property
    def sql_select(self):
        return """
            SELECT COUNT(DISTINCT action_receipt_actions.receipt_receiver_account_id)
            FROM action_receipt_actions
            WHERE action_receipt_actions.receipt_included_in_block_timestamp >= %(from_timestamp)s
                AND action_receipt_actions.receipt_included_in_block_timestamp < %(to_timestamp)s
                AND action_receipt_actions.action_kind = 'FUNCTION_CALL'
        """

    @property
    def sql_insert(self):
        return """
            INSERT INTO daily_active_contracts_count VALUES %s
            ON CONFLICT DO NOTHING
        """

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, timestamp: int) -> int:
        return daily_start_of_range(timestamp)
