import datetime

from . import DAY_LEN_SECONDS, daily_start_of_range
from ..periodic_aggregations import PeriodicAggregations


class DailyIngoingTransactionsPerAccountCount(PeriodicAggregations):
    @property
    def sql_create_table(self):
        # Suppose we have at most 10^5 (100K) transactions per second.
        # In the worst case, they are all from one account.
        # It gives ~10^10 transactions per day.
        # It means we fit into BIGINT (10^18)
        return '''
            CREATE TABLE IF NOT EXISTS daily_ingoing_transactions_per_account_count
            (
                collected_for_day          DATE   NOT NULL,
                account_id                 TEXT   NOT NULL,
                ingoing_transactions_count BIGINT NOT NULL,
                CONSTRAINT daily_ingoing_transactions_per_account_count_pk PRIMARY KEY (collected_for_day, account_id)
            );
            CREATE INDEX IF NOT EXISTS daily_ingoing_transactions_per_account_count_idx
                ON daily_ingoing_transactions_per_account_count (collected_for_day, ingoing_transactions_count DESC)
        '''

    @property
    def sql_drop_table(self):
        return '''
            DROP TABLE IF EXISTS daily_ingoing_transactions_per_account_count
        '''

    @property
    def sql_select(self):
        # Ingoing transactions for user X aren't only transactions where receiver_account_id == X.
        # We need to find all chains with receipts where X was the receiver.
        # It's important to add 10 minutes to receipt border, we should pack the transaction
        # with all their receipts together, or the numbers will not be accurate.
        # Other receipts from the next day will be naturally ignored.
        # Transactions border remains the same, taking only transactions for the specified day.
        # If you want to change 10 minutes constant, fix it also in PeriodicAggregations.is_indexer_ready
        return '''
            SELECT
                receipts.receiver_account_id,
                COUNT(DISTINCT transactions.transaction_hash) AS transactions_count
            FROM transactions
            LEFT JOIN receipts ON receipts.originated_from_transaction_hash = transactions.transaction_hash
                AND transactions.block_timestamp >= %(from_timestamp)s
                AND transactions.block_timestamp < %(to_timestamp)s
            WHERE receipts.included_in_block_timestamp >= %(from_timestamp)s
                AND receipts.included_in_block_timestamp < (%(to_timestamp)s + 600000000000)
                AND transactions.signer_account_id != receipts.receiver_account_id 
            GROUP BY receipts.receiver_account_id
        '''

    @property
    def sql_insert(self):
        return '''
            INSERT INTO daily_ingoing_transactions_per_account_count VALUES %s
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
        return [(computed_for, account_id, count) for (account_id, count) in parameters]
