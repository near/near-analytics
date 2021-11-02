import csv
from ..sql_aggregations import SqlAggregations


class EcosystemEntities(SqlAggregations):
    @property
    def sql_create_table(self):
        # All fields are text, of varying length
        return '''
            CREATE TABLE IF NOT EXISTS ecosystem_entities
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
            DROP TABLE IF EXISTS ecosystem_entities
        '''
    @property
    def sql_select(self):
        return "manual"          

    @property
    def sql_insert(self):
        return '''
            INSERT INTO ecosystem_entities VALUES %s
            ON CONFLICT DO NOTHING
        '''  

#overload collect method defined in sql_aggregations
    @staticmethod
    def collect(self, requested_timestamp = "string", overload=1):
        with open("aggregations/csv/near_apps.csv") as csvFile:   
            read = csv.reader(csvFile, delimiter=',')
            result = list(read)
            csvFile.close()
            return result
      