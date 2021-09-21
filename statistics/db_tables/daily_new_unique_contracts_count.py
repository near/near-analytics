import datetime
import time
import typing

from . import DAY_LEN_SECONDS, daily_start_of_range, GENESIS_SECONDS
from ..periodic_statistics import PeriodicStatistics
from ..statistics import to_nanos


# This metric depends both on Indexer data, and on `deployed_contracts` table in Analytics DB.
class DailyNewUniqueContractsCount(PeriodicStatistics):
    @property
    def sql_create_table(self):
        # For September 2021, we have 10^6 accounts on the Mainnet.
        # It means we fit into integer (10^9)
        return '''
            CREATE TABLE IF NOT EXISTS daily_new_unique_contracts_count
            (
                collected_for_day          DATE PRIMARY KEY,
                new_unique_contracts_count INTEGER NOT NULL
            )
        '''

    @property
    def sql_drop_table(self):
        return '''
            DROP TABLE IF EXISTS daily_new_unique_contracts_count
        '''

    @property
    def sql_select(self):
        return '''
            SELECT DISTINCT args->>'code_sha256' as code_sha256 
            FROM action_receipt_actions
            JOIN receipts ON receipts.receipt_id = action_receipt_actions.receipt_id
            WHERE receipts.included_in_block_timestamp >= %(from_timestamp)s
                AND receipts.included_in_block_timestamp < %(to_timestamp)s
                AND action_kind = 'DEPLOY_CONTRACT'
        '''

    @property
    def sql_select_all(self):
        # It's not a good idea to calculate it through one select
        # We have to fill it by for-cycle on Python level
        raise NotImplementedError

    @property
    def sql_insert(self):
        return '''
            INSERT INTO daily_new_unique_contracts_count VALUES %s
            ON CONFLICT DO NOTHING
        '''

    def collect_statistics(self, requested_timestamp: typing.Optional[int], collect_all: bool) -> list:
        if collect_all:
            return self.collect_all_data()

        # Get new contracts from Indexer DB. We use `distinct` in SQL,
        # But we still have no guarantees because the contract could be added a week ago
        new_contracts = super().collect_statistics(requested_timestamp, collect_all=False)

        all_contract_hashes_select = '''
            SELECT code_sha256
            FROM deployed_contracts
            WHERE created_by_block_timestamp < %(timestamp)s
        '''

        from_timestamp = self.start_of_range(requested_timestamp)
        with self.analytics_connection.cursor() as analytics_cursor:
            # Get all contracts that were added before our time range
            analytics_cursor.execute(all_contract_hashes_select, {'timestamp': to_nanos(from_timestamp)})
            previous_contracts = analytics_cursor.fetchall()

            # Find truly unique contracts
            new_unique_contracts = [c for c in new_contracts if c not in previous_contracts]

            time = datetime.datetime.utcfromtimestamp(from_timestamp).strftime('%Y-%m-%d')
            return [(time, len(new_unique_contracts))]

    def collect_all_data(self):
        current = GENESIS_SECONDS
        result = []
        # TODO check for the last day
        while current < int(time.time()):
            value = self.collect_statistics(current, False)
            print(value)
            result.extend(value)
            current += DAY_LEN_SECONDS
        return result

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, requested_statistics_timestamp: typing.Optional[int]) -> int:
        return daily_start_of_range(requested_statistics_timestamp)

    @staticmethod
    def prepare_data(parameters, **kwargs) -> list:
        return parameters
