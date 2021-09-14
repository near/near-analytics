import abc
import dataclasses
import psycopg2
import typing


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


def to_nanos(timestamp_seconds):
    return timestamp_seconds * 1000 * 1000 * 1000


def time_range_json(from_timestamp, duration):
    return {
        "from_timestamp": to_nanos(from_timestamp),
        "to_timestamp": to_nanos(from_timestamp + duration)
    }
