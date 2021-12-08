import json
import requests

from ..sql_aggregations import SqlAggregations


class NearEcosystemEntities(SqlAggregations):
    @property
    def sql_create_table(self):
        return """
            CREATE TABLE IF NOT EXISTS near_ecosystem_entities
            (
                slug     TEXT PRIMARY KEY,
                title    TEXT,
                oneliner TEXT,
                website  TEXT,
                category TEXT,
                status   TEXT,
                contract TEXT,
                logo     TEXT,
                is_app   BOOLEAN,
                is_nft   BOOLEAN,
                is_guild BOOLEAN,
                is_defi  BOOLEAN,
                is_dao   BOOLEAN
            )
        """

    @property
    def sql_drop_table(self):
        return """
            DROP TABLE IF EXISTS near_ecosystem_entities
        """

    @property
    def sql_select(self):
        raise NotImplementedError(
            "No requests to Indexer DB needed for near_ecosystem_entities"
        )

    @property
    def sql_insert(self):
        return """
            INSERT INTO near_ecosystem_entities VALUES %s
            ON CONFLICT DO NOTHING
        """

    def collect(self, requested_timestamp: int) -> list:
        # Dirty hack to enforce the DB rewrite all the data each time
        self.drop_table()
        self.create_table()

        url = "https://raw.githubusercontent.com/near/ecosystem/main/entities.json"
        data = json.loads(requests.get(url).text)

        return [
            [
                record.get("slug"),
                record.get("title"),
                record.get("oneliner"),
                record.get("website"),
                record.get("category"),
                record.get("status"),
                record.get("contract"),
                record.get("logo"),
                "app" in record["category"],
                "nft" in record["category"],
                "guild" in record["category"],
                "defi" in record["category"],
                "dao" in record["category"],
            ]
            for record in data
        ]
