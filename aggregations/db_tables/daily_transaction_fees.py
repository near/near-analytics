from . import DAY_LEN_SECONDS, daily_start_of_range
from ..periodic_aggregations import PeriodicAggregations


class DailyTransactionFees(PeriodicAggregations):
    @property
    def sql_create_table(self):
        # Use numeric(30,0) per discussion in https://github.com/near/near-analytics/issues/17.
        return """
            CREATE TABLE IF NOT EXISTS daily_transaction_fees
            (
                collected_for_day   DATE PRIMARY KEY,
                transaction_fees    numeric(30, 0) NOT NULL
            )
        """

    @property
    def sql_drop_table(self):
        return """
            DROP TABLE IF EXISTS daily_transaction_fees
        """

    @property
    def sql_select(self):
        return """
            SELECT SUM(chunks.gas_used * blocks.gas_price)
            FROM blocks
            JOIN chunks ON chunks.included_in_block_hash = blocks.block_hash
            WHERE blocks.block_timestamp >= %(from_timestamp)s
                AND blocks.block_timestamp < %(to_timestamp)s
        """

    @property
    def sql_insert(self):
        return """
            INSERT INTO daily_transaction_fees VALUES %s
            ON CONFLICT DO NOTHING
        """

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, timestamp: int) -> int:
        return daily_start_of_range(timestamp)
