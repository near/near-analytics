import csv
from ..sql_aggregations import SqlAggregations

class ManualAppCsv(SqlAggregations):
    @property
    def sql_create_table(self):
        # All fields are text, of varying length
        return '''
            CREATE TABLE IF NOT EXISTS manual_app_csv
            (
                slug        TEXT PRIMARY KEY,
                title       TEXT,
                oneliner    TEXT,      
                website     TEXT, 
                category    TEXT,    
                status      TEXT, 
                contract    TEXT
            )
        '''

    @property
    def sql_drop_table(self):
        return '''
            DROP TABLE IF EXISTS manual_app_csv
        '''

    @property
    def sql_select(self):
        return "manual"

    @property
    def sql_insert(self):
        return '''
            INSERT INTO manual_app_csv VALUES %s
            ON CONFLICT DO NOTHING
        '''