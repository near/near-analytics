import abc
import sys

import psycopg2
import psycopg2.extras
import typing

from .base_statistics import BaseStatistics
from .db_tables import time_json


class Aggregations(BaseStatistics):
    def dependencies(self) -> list:
        return []

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

    def create_table(self):
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

    def collect_statistics(self, requested_timestamp: typing.Optional[int]) -> list:
        with self.indexer_connection.cursor() as indexer_cursor:
            select = self.sql_select if requested_timestamp else self.sql_select_all
            indexer_cursor.execute(select, time_json(requested_timestamp or sys.maxsize))
            result = indexer_cursor.fetchall()
            return self.prepare_data(result)

    def store_statistics(self, parameters: list):
        chunk_size = 100
        with self.analytics_connection.cursor() as analytics_cursor:
            for i in range(0, len(parameters), chunk_size):
                try:
                    psycopg2.extras.execute_values(analytics_cursor, self.sql_insert, parameters[i:i + chunk_size])
                    self.analytics_connection.commit()
                except psycopg2.errors.UniqueViolation:
                    self.analytics_connection.rollback()

    # Overload this method if you need to prepare data before insert
    @staticmethod
    def prepare_data(parameters, **kwargs) -> list:
        return parameters
