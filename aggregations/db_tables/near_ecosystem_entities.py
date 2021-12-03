import json,requests
from ..sql_aggregations import SqlAggregations


class NearEcosystemEntities(SqlAggregations):
    @property
    def sql_create_table(self):
        return """
            CREATE TABLE IF NOT EXISTS near_ecosystem_entities
            (
                slug         TEXT PRIMARY KEY
                ,title       TEXT
                ,oneliner    TEXT      
                ,website     TEXT 
                ,category    TEXT    
                ,status      TEXT 
                ,contract    TEXT
                ,logo        TEXT
                ,is_app      BOOLEAN
                ,is_nft      BOOLEAN
                ,is_guild    BOOLEAN
                ,is_defi     BOOLEAN
                ,is_dao      BOOLEAN  
                                                
            )
        """

    @property
    def sql_drop_table(self):
        return """
            DROP TABLE IF EXISTS near_ecosystem_entities
        """

    @property
    def sql_select(self):
        pass

    @property
    def sql_insert(self):
        return """
            INSERT INTO near_ecosystem_entities VALUES(%s %s %s %s %s %s %s %s %s %s %s, %s, %s, %s, %s, %s) 
            ON CONFLICT DO NOTHING
        """

    def collect(self, requested_timestamp: int):
        pass

    def store(self, parameters: list):
        url = "https://raw.githubusercontent.com/near/ecosystem/main/entities.json"
        r = requests.get(url).text
        data = []
        data = json.loads(r)

        sql_insert = """
            INSERT INTO near_ecosystem_entities VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
            ON CONFLICT DO NOTHING
        """

        with self.analytics_connection.cursor() as analytics_cursor:
            #Remove & update schema
            try:
                analytics_cursor.execute(self.sql_drop_table)
                self.analytics_connection.commit()
            except psycopg2.errors.UndefinedTable:
                self.analytics_connection.rollback()
            try:
                analytics_cursor.execute(self.sql_create_table)
                self.analytics_connection.commit()
            except psycopg2.errors.DuplicateTable:
                self.analytics_connection.rollback()


            for record in data:
                app = ('app' in record['category'])    
                nft = ('nft' in record['category'])  
                guild = ('guild' in record['category'])  
                defi = ('defi' in record['category'])  
                dao = ('dao' in record['category']) 
                analytics_cursor.execute(
                    sql_insert, [record.get('slug'), record.get('title'), record.get('oneliner'), record.get('website'), record.get('category'), record.get('status'), record.get('contract'), record.get('logo'), app, nft, guild, defi, dao]
                )