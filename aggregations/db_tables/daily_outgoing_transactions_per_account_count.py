import datetime

from . import DAY_LEN_SECONDS, daily_start_of_range
from ..periodic_aggregations import PeriodicAggregations


class DailyOutgoingTransactionsPerAccountCount(PeriodicAggregations):
    @property
    def sql_create_table(self):
        # Suppose we have at most 10^5 (100K) transactions per second.
        # In the worst case, they are all from one account.
        # It gives ~10^10 transactions per day.
        # It means we fit into BIGINT (10^18)
        return """
            CREATE TABLE IF NOT EXISTS daily_outgoing_transactions_per_account_count
            (
                collected_for_day           DATE   NOT NULL,
                account_id                  TEXT   NOT NULL,
                outgoing_transactions_count BIGINT NOT NULL,
                CONSTRAINT daily_outgoing_transactions_per_account_count_pk PRIMARY KEY (collected_for_day, account_id)
            );
            CREATE INDEX IF NOT EXISTS daily_outgoing_transactions_per_account_count_idx
                ON daily_outgoing_transactions_per_account_count (account_id, outgoing_transactions_count);
            CREATE INDEX IF NOT EXISTS daily_outgoing_transactions_chart_idx
                ON daily_outgoing_transactions_per_account_count (collected_for_day, outgoing_transactions_count)
        """

    @property
    def sql_drop_table(self):
        return """
            DROP TABLE IF EXISTS daily_outgoing_transactions_per_account_count
        """

    @property
    def sql_select(self):
        return """
            SELECT
                signer_account_id,
                COUNT(*) AS outgoing_transactions_count
            FROM transactions
            WHERE transactions.block_timestamp >= %(from_timestamp)s
                AND transactions.block_timestamp < %(to_timestamp)s
            GROUP BY signer_account_id
        """

    @property
    def sql_insert(self):
        return """
            INSERT INTO daily_outgoing_transactions_per_account_count VALUES %s
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
        return [(computed_for, account_id, count) for (account_id, count) in parameters]
