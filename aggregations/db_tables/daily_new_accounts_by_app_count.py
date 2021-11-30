import datetime

from . import DAY_LEN_SECONDS, daily_start_of_range, time_json
from ..sql_aggregations import SqlAggregations


class DailyNewAccountsByAppCount(SqlAggregations):
    def dependencies(self) -> list:
        return ["daily_add_key_by_app_count"]

    @property
    def sql_create_table(self):
        return """
            CREATE TABLE IF NOT EXISTS daily_new_accounts_by_app_count
            (
                collected_for_day  DATE NOT NULL,
                app_id TEXT NOT NULL,
                new_accounts_count INTEGER NOT NULL,
                PRIMARY KEY (collected_for_day, app_id )
            );
            CREATE INDEX IF NOT EXISTS daily_new_accounts_by_app_count_idx
                ON daily_new_accounts_by_app_count (collected_for_day, new_accounts_count DESC)
        """

    @property
    def sql_drop_table(self):
        return """
            DROP TABLE IF EXISTS daily_new_accounts_by_app_count
        """

    @property
    def sql_select(self):
        pass

    def collect(self, requested_timestamp: int) -> list:
        first_date_sql = """
            SELECT
            first_date AS collected_for_day,
            app_id,
            COUNT(*)
            FROM (
                    SELECT 
                    app_id,
                    receipt_receiver_account_id,
                    min(collected_for_day) as first_date
                    FROM public.daily_add_key_by_app_count
                    WHERE app_id != 'All Others'
                    GROUP BY 1,2
            ) t 
            GROUP BY 1,2
        """

        with self.analytics_connection.cursor() as analytics_cursor:
            analytics_cursor.execute(first_date_sql)
            first_date = analytics_cursor.fetchall()
            return first_date

    @property
    def sql_insert(self):
        return """
            INSERT INTO daily_new_accounts_by_app_count VALUES %s
            ON CONFLICT DO NOTHING
        """

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, timestamp: int) -> int:
        return daily_start_of_range(timestamp)

    @staticmethod
    def prepare_data(parameters: list, **kwargs) -> list:
        computed_for = datetime.datetime.utcfromtimestamp(
            kwargs["start_of_range"]
        ).strftime("%Y-%m-%d")
        return [
            (computed_for, app_id, new_accounts)
            for (app_id, new_accounts) in parameters
        ]
