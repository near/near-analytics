import datetime

from . import DAY_LEN_SECONDS, daily_start_of_range
from ..periodic_aggregations import PeriodicAggregations


# It's not cumulative. top_of_range_in_teragas == 200 means the range 150-200
class DailyTransactionCountByGasBurntRanges(PeriodicAggregations):
    @property
    def sql_create_table(self):
        # Suppose we have at most 10^5 (100K) transactions per second.
        # In the worst case, they are all in the one range
        # It gives ~10^10 transactions per day.
        # It means we fit into BIGINT (10^18)
        return """
            CREATE TABLE IF NOT EXISTS daily_transaction_count_by_gas_burnt_ranges
            (
                collected_for_day       DATE    NOT NULL,
                top_of_range_in_teragas INTEGER NOT NULL,
                transactions_count      BIGINT  NOT NULL,
                CONSTRAINT daily_transaction_count_by_gas_burnt_ranges_pk PRIMARY KEY (collected_for_day, top_of_range_in_teragas)
            )
        """

    @property
    def sql_drop_table(self):
        return """
            DROP TABLE IF EXISTS daily_transaction_count_by_gas_burnt_ranges
        """

    @property
    def sql_select(self):
        return """
            SELECT (range_in_teragas + 1) * 50, count(transaction_hash)
            FROM (
                SELECT receipts.originated_from_transaction_hash AS transaction_hash,
                    round(div(sum(execution_outcomes.gas_burnt), CAST(power(10, 12) * 50 AS BIGINT))) as range_in_teragas
                FROM execution_outcomes JOIN receipts ON receipts.receipt_id = execution_outcomes.receipt_id
                WHERE receipts.included_in_block_timestamp >= %(from_timestamp)s
                    AND receipts.included_in_block_timestamp < %(to_timestamp)s
                GROUP BY receipts.originated_from_transaction_hash
            ) a
            GROUP BY range_in_teragas
        """

    @property
    def sql_insert(self):
        return """
            INSERT INTO daily_transaction_count_by_gas_burnt_ranges VALUES %s
            ON CONFLICT DO NOTHING
        """

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, timestamp: int) -> int:
        return daily_start_of_range(timestamp)

    @staticmethod
    def prepare_data(parameters: list, *, start_of_range=None, **kwargs) -> list:
        computed_for = datetime.datetime.utcfromtimestamp(start_of_range).strftime(
            "%Y-%m-%d"
        )
        return [
            (computed_for, top_of_range, count) for (top_of_range, count) in parameters
        ]
