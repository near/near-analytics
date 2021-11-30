import csv
from ..sql_aggregations import SqlAggregations
import psycopg2
import psycopg2.extras


class NearEcosystemEntities(SqlAggregations):
    @property
    def sql_create_table(self):
        return """
            CREATE TABLE IF NOT EXISTS near_ecosystem_entities
            (
                slug        TEXT PRIMARY KEY,
                title       TEXT,
                oneliner    TEXT,      
                website     TEXT, 
                category    TEXT,    
                status      TEXT, 
                contract    TEXT,
                logo        TEXT,
                app         BOOLEAN,
                nft         BOOLEAN,
                guild       BOOLEAN 
 
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
            INSERT INTO near_ecosystem_entities VALUES %s
            ON CONFLICT DO NOTHING
        """

    def collect(self, requested_timestamp: int):
        with open("aggregations/csv/near_ecosystem_entities.csv") as csvFile:
            read = csv.reader(csvFile, delimiter=",")
            result = list(read)
            csvFile.close()
            csv_columns_count = len(result[0])

            with self.analytics_connection.cursor() as analytics_cursor:
                sql_columns = """
                SELECT count(*) as columns 
                FROM information_schema.columns 
                WHERE table_name='near_ecosystem_entities'
                """

                analytics_cursor.execute(sql_columns)
                sql_columns_count = analytics_cursor.fetchone()[0]
                if sql_columns_count != csv_columns_count:
                    print("updating table definition for near_ecosystem_entities")
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
            return result
