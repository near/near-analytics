import datetime

from . import DAY_LEN_SECONDS, daily_start_of_range
from ..periodic_aggregations import PeriodicAggregations


class DailyAddKeysPerEntityAccountCount(PeriodicAggregations):
    @property
    def sql_create_table(self):
        # Suppose we have at most 10^5 (100K) transactions per second.
        # In the worst case, they are all from one account.
        # It gives ~10^10 transactions per day.
        # It means we fit into BIGINT (10^18)
        return '''
            CREATE TABLE IF NOT EXISTS daily_add_keys_per_entity_account_count
            (
                collected_for_day  DATE NOT NULL,
                receiver_account_id TEXT NOT NULL,
                add_keys_count INTEGER NOT NULL,
                PRIMARY KEY (collected_for_day, receiver_account_id )
            );
            CREATE INDEX IF NOT EXISTS daily_add_keys_per_entity_account_count_idx
                ON daily_add_keys_per_entity_account_count (collected_for_day, add_keys_count DESC)
        '''

    @property
    def sql_drop_table(self):
        return '''
            DROP TABLE IF EXISTS daily_add_keys_per_entity_account_count
        '''

    @property
    def sql_select(self):
        return '''
            SELECT args ->'access_key' -> 'permission' -> 'permission_details' ->> 'receiver_id' as receiver_account_id ,
            count(*) as add_keys_count
                FROM public.action_receipt_actions
                WHERE action_kind = 'ADD_KEY'
                AND args ->'access_key' -> 'permission' ->> 'permission_kind' = 'FUNCTION_CALL'
                AND args ->'access_key' -> 'permission' -> 'permission_details' ->> 'receiver_id' != receipt_receiver_account_id 
                AND args ->'access_key' -> 'permission' -> 'permission_details' ->> 'receiver_id' != 'near'    
                AND receipt_included_in_block_timestamp >= %(from_timestamp)s
                AND receipt_included_in_block_timestamp < %(to_timestamp)s
                GROUP BY args ->'access_key' -> 'permission' -> 'permission_details' ->> 'receiver_id'
        '''

    @property
    def sql_insert(self):
        return '''
            INSERT INTO daily_add_keys_per_entity_account_count VALUES %s
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
        return [(computed_for, receiver_account_id, add_keys_count) for (receiver_account_id , add_keys_count) in parameters]
