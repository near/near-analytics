import abc
import dataclasses
import typing
import psycopg2


@dataclasses.dataclass
class Statistics(abc.ABC):
    analytics_connection: psycopg2.extensions.connection
    indexer_connection: psycopg2.extensions.connection

    @abc.abstractmethod
    def create_tables(self):
        pass

    @abc.abstractmethod
    def collect_statistics(self, requested_statistics_timestamp: typing.Optional[int]) -> dict:
        pass

    @abc.abstractmethod
    def store_statistics(self, parameters: dict):
        pass
