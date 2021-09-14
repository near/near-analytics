import abc
import datetime
import psycopg2
import typing

from .statistics import Statistics, to_nanos


class SqlStatistics(Statistics):
    @property
    @abc.abstractmethod
    def sql_create_table(self):
        pass

    @property
    @abc.abstractmethod
    def sql_select(self):
        pass

    @property
    @abc.abstractmethod
    def sql_insert(self):
        pass

    @abc.abstractmethod
    def start_of_range(self, requested_statistics_timestamp: typing.Optional[int]) -> int:
        pass

    @property
    @abc.abstractmethod
    def duration_seconds(self) -> int:
        pass

    def create_tables(self):
        with self.analytics_connection.cursor() as analytics_cursor:
            try:
                analytics_cursor.execute(self.sql_create_table)
                self.analytics_connection.commit()
            except psycopg2.errors.DuplicateTable:
                self.analytics_connection.rollback()

    def collect_statistics(self, requested_statistics_timestamp: typing.Optional[int]) -> dict:
        from_timestamp = self.start_of_range(requested_statistics_timestamp)
        to_timestamp = from_timestamp + self.duration_seconds
        sql_parameters = {
            "from_timestamp": to_nanos(from_timestamp),
            "to_timestamp": to_nanos(to_timestamp)
        }
        with self.indexer_connection.cursor() as indexer_cursor:
            indexer_cursor.execute(self.sql_select, sql_parameters)
            result = indexer_cursor.fetchone()[0] or 0
            time = datetime.datetime.utcfromtimestamp(from_timestamp).strftime('%Y-%m-%d')
            return {"time": time, "result": result}

    def store_statistics(self, parameters: dict):
        with self.analytics_connection.cursor() as analytics_cursor:
            try:
                analytics_cursor.execute(self.sql_insert, parameters)
                self.analytics_connection.commit()
            except psycopg2.errors.UniqueViolation:
                self.analytics_connection.rollback()
