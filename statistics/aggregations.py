import abc
import psycopg2
import psycopg2.extras
import typing

from .statistics import Statistics, to_nanos


class Aggregations(Statistics):
    @property
    @abc.abstractmethod
    def sql_create_table(self):
        pass

    @property
    @abc.abstractmethod
    def sql_drop_table(self):
        pass

    @property
    @abc.abstractmethod
    def sql_select(self):
        pass

    @property
    @abc.abstractmethod
    def sql_select_all(self):
        pass

    @property
    @abc.abstractmethod
    def sql_insert(self):
        pass

    def create_table(self, drop_previous_table: bool):
        if drop_previous_table:
            self.drop_table()

        with self.analytics_connection.cursor() as analytics_cursor:
            try:
                analytics_cursor.execute(self.sql_create_table)
                self.analytics_connection.commit()
            except psycopg2.errors.DuplicateTable:
                self.analytics_connection.rollback()

    def drop_table(self):
        with self.analytics_connection.cursor() as analytics_cursor:
            try:
                analytics_cursor.execute(self.sql_drop_table)
                self.analytics_connection.commit()
            except psycopg2.errors.UndefinedTable:
                self.analytics_connection.rollback()

    def collect_statistics(self, requested_timestamp: typing.Optional[int], collect_all: bool) -> list:
        with self.indexer_connection.cursor() as indexer_cursor:
            select = self.sql_select_all if collect_all else self.sql_select
            indexer_cursor.execute(select, {'timestamp': to_nanos(requested_timestamp or 0)})
            result = indexer_cursor.fetchall()
            return self.prepare_data(result)

    def store_statistics(self, parameters: list):
        with self.analytics_connection.cursor() as analytics_cursor:
            try:
                psycopg2.extras.execute_values(analytics_cursor, self.sql_insert, parameters)
                self.analytics_connection.commit()
            except psycopg2.errors.UniqueViolation:
                self.analytics_connection.rollback()

    @staticmethod
    def prepare_data(parameters, **kwargs) -> list:
        return parameters
