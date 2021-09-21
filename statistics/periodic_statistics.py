import abc
import datetime
import typing

from .aggregations import Aggregations
from .statistics import time_range_json


class PeriodicStatistics(Aggregations):
    @abc.abstractmethod
    def start_of_range(self, requested_statistics_timestamp: typing.Optional[int]) -> int:
        pass

    @property
    @abc.abstractmethod
    def duration_seconds(self) -> int:
        pass

    def collect_statistics(self, requested_timestamp: typing.Optional[int], collect_all: bool) -> list:
        from_timestamp = self.start_of_range(requested_timestamp)
        with self.indexer_connection.cursor() as indexer_cursor:
            select = self.sql_select_all if collect_all else self.sql_select
            indexer_cursor.execute(select, time_range_json(from_timestamp, self.duration_seconds))
            result = indexer_cursor.fetchall()
            return self.prepare_data(result, start_of_range=from_timestamp)

    @staticmethod
    def prepare_data(parameters, **kwargs) -> list:
        # We usually have one-value returns, and we need to merge it with corresponding date
        if len(parameters) == 1:
            time = datetime.datetime.utcfromtimestamp(kwargs['start_of_range'])
            parameters = [(time, parameters[0][0] or 0)]
        return [(collected_for.strftime('%Y-%m-%d'), data) for (collected_for, data) in parameters]
