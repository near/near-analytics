import datetime

from . import DAY_LEN_SECONDS, daily_start_of_range, time_json
from ..periodic_aggregations import PeriodicAggregations


class DailyNewAccountsByAppCount(PeriodicAggregations):
    @property
    def sql_create_table(self):
        # Suppose we have at most 10^5 (100K) transactions per second.
        # In the worst case, they are all from one account.
        # It gives ~10^10 transactions per day.
        # It means we fit into BIGINT (10^18)
        return '''
            CREATE TABLE IF NOT EXISTS daily_new_accounts_by_app_count
            (
                collected_for_day  DATE NOT NULL,
                app_id TEXT NOT NULL,
                new_accounts INTEGER NOT NULL,
                PRIMARY KEY (collected_for_day, app_id )
            );
            CREATE INDEX IF NOT EXISTS daily_new_accounts_by_app_count_idx
                ON daily_new_accounts_by_app_count (collected_for_day, new_accounts DESC)
        '''

    @property
    def sql_drop_table(self):
        return '''
            DROP TABLE IF EXISTS daily_new_accounts_by_app_count
        '''

    @property
    def sql_select(self):
        # retrieve app data from near- analytics
        all_apps = '''
            SELECT token, slug as app_id
            FROM public.near_ecosystem_entities e, unnest(string_to_array(e.contract, ', ')) s(token)
            WHERE category LIKE '%%app%%' AND length(contract)>0
        '''

        with self.analytics_connection.cursor() as analytics_cursor:
            # Get all contracts that were added before our time range
            analytics_cursor.execute(all_apps)
            app_contracts = analytics_cursor.fetchall()

        #concat table values into case statement
        string = ''
        for index, tuple in enumerate(app_contracts):
#            contract = ''
            contract = str(tuple[0])
            app = str(tuple[1])
            string += str("WHEN args ->'access_key' -> 'permission' -> 'permission_details' ->> 'receiver_id' LIKE '%%' ||'"  + contract.strip() + "' THEN '" +  app.strip() + "'")
        
        opener = '''SELECT CASE '''
        closer = '''ELSE 'All Others' END as app_id ,
        count(*) as new_accounts
        FROM public.action_receipt_actions
        WHERE action_kind = 'ADD_KEY'
            AND args ->'access_key' -> 'permission' ->> 'permission_kind' = 'FUNCTION_CALL'
            AND args ->'access_key' -> 'permission' -> 'permission_details' ->> 'receiver_id' != receipt_receiver_account_id 
            AND args ->'access_key' -> 'permission' -> 'permission_details' ->> 'receiver_id' != 'near'    
            AND receipt_included_in_block_timestamp >= %(from_timestamp)s
            AND receipt_included_in_block_timestamp < %(to_timestamp)s
        GROUP BY 1
        '''
        return str(opener) + str(string) + str(closer)
        
    @property
    def sql_insert(self):
        return '''
            INSERT INTO daily_new_accounts_by_app_count VALUES %s
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
        return [(computed_for, app_id, new_accounts) for (app_id , new_accounts) in parameters]