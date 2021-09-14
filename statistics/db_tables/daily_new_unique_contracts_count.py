import datetime
import typing

from psycopg2.extras import execute_values

from . import DAY_LEN_SECONDS, daily_start_of_range
from ..sql_statistics import SqlStatistics
from ..statistics import to_nanos, time_range_json


# This metric depends both on Indexer data, and on `contracts` table in Analytics DB.
class DailyNewUniqueContractsCount(SqlStatistics):
    @property
    def sql_create_table(self):
        # For September 2021, we have 10^6 accounts on the Mainnet.
        # It means we fit into integer (10^9)
        return '''
            CREATE TABLE IF NOT EXISTS contracts
            (
                code_sha256                text PRIMARY KEY,
                contract_id                text           NOT NULL,
                created_by_receipt_id      text           NOT NULL,
                created_by_block_timestamp numeric(20, 0) NOT NULL
            );
            CREATE TABLE IF NOT EXISTS daily_new_unique_contracts_count
            (
                collected_for_day          DATE PRIMARY KEY,
                new_unique_contracts_count INTEGER NOT NULL
            )
        '''

    def create_tables(self):
        super().create_tables()

        last_previously_found_contract_select = '''
            SELECT created_by_block_timestamp
            FROM contracts
            ORDER BY contracts.created_by_block_timestamp DESC
            LIMIT 1
        '''

        new_contracts_select = '''
            SELECT action_receipt_actions.args->>'code_sha256' as code_sha256,
                receipts.receiver_account_id as contract_id, receipts.receipt_id as created_by_receipt_id,
                receipts.included_in_block_timestamp as created_by_block_timestamp
            FROM action_receipt_actions
            JOIN receipts ON receipts.receipt_id = action_receipt_actions.receipt_id
            WHERE receipts.included_in_block_timestamp >= %(timestamp)s
                AND action_kind = 'DEPLOY_CONTRACT'
            ORDER BY receipts.included_in_block_timestamp
        '''

        contracts_insert = '''
            INSERT INTO contracts values %s 
            ON CONFLICT DO NOTHING
        '''

        with self.analytics_connection.cursor() as analytics_cursor, \
                self.indexer_connection.cursor() as indexer_cursor:
            # Find timestamp (*) of last added unique contract in Analytics DB
            analytics_cursor.execute(last_previously_found_contract_select)
            timestamp = analytics_cursor.fetchone()
            timestamp = timestamp[0] if timestamp else 0

            # Collect all contracts created after (*) from Indexer DB
            indexer_cursor.execute(new_contracts_select, {'timestamp': timestamp})
            contracts = indexer_cursor.fetchall()

            # Save all new contracts into contracts table in Analytics DB
            # The table will get rid of duplicates, because contract hash is a primary key
            execute_values(analytics_cursor, contracts_insert, contracts)
            self.analytics_connection.commit()

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

    def collect_statistics(self, requested_statistics_timestamp: typing.Optional[int]) -> dict:
        all_contract_hashes_list_select = '''
            SELECT code_sha256
            FROM contracts
            WHERE created_by_block_timestamp < %(timestamp)s
        '''

        from_timestamp = self.start_of_range(requested_statistics_timestamp)
        with self.analytics_connection.cursor() as analytics_cursor, \
                self.indexer_connection.cursor() as indexer_cursor:
            # Get new contracts from Indexer DB. We use `distinct` in SQL,
            # But we still have no guarantees because the contract could be added a week ago
            indexer_cursor.execute(self.sql_select, time_range_json(from_timestamp, self.duration_seconds))
            new_contracts = indexer_cursor.fetchall()

            # Get all contracts that were added before our time range
            analytics_cursor.execute(all_contract_hashes_list_select, {'timestamp': to_nanos(from_timestamp)})
            previous_contracts = analytics_cursor.fetchall()

            # Find truly unique contracts
            new_unique_contracts = [c for c in new_contracts if c not in previous_contracts]

            time = datetime.datetime.utcfromtimestamp(from_timestamp).strftime('%Y-%m-%d')
            return {"time": time, "result": len(new_unique_contracts)}

    @property
    def sql_insert(self):
        return '''
            INSERT INTO daily_new_unique_contracts_count VALUES (
                %(time)s,
                %(result)s
            )
        '''

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, requested_statistics_timestamp: typing.Optional[int]) -> int:
        return daily_start_of_range(requested_statistics_timestamp)
