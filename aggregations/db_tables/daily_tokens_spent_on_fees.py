from . import DAY_LEN_SECONDS, daily_start_of_range
from ..periodic_aggregations import PeriodicAggregations


class DailyTokensSpentOnFees(PeriodicAggregations):
    @property
    def sql_create_table(self):
        # In Indexer, we store all the balances in numeric(45,0), including total_supply.
        # Assuming the users spend on fees not more than total_supply inside each second,
        # I suggest to use numeric(50, 0)
        return """
            CREATE TABLE IF NOT EXISTS daily_tokens_spent_on_fees
            (
                collected_for_day   DATE PRIMARY KEY,
                tokens_spent_on_fees    numeric(50, 0) NOT NULL
            )
        """

    @property
    def sql_drop_table(self):
        return """
            DROP TABLE IF EXISTS daily_tokens_spent_on_fees
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
            INSERT INTO daily_tokens_spent_on_fees VALUES %s
            ON CONFLICT DO NOTHING
        """

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, timestamp: int) -> int:
        return daily_start_of_range(timestamp)
