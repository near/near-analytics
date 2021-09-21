import abc
import datetime
import time
import typing

from .aggregations import Aggregations
from .db_tables import time_range_json


class PeriodicStatistics(Aggregations):
    @abc.abstractmethod
    def start_of_range(self, requested_statistics_timestamp: typing.Optional[int]) -> int:
        pass

    @property
    @abc.abstractmethod
    def duration_seconds(self) -> int:
        pass

    # requested_timestamp will be rounded to the start of the day, week (Monday), month, etc.
    def collect_statistics(self, requested_timestamp: typing.Optional[int]) -> list:
        from_timestamp = self.start_of_range(requested_timestamp)
        if from_timestamp + self.duration_seconds > int(time.time()):
            return []
        with self.indexer_connection.cursor() as indexer_cursor:
            select = self.sql_select if requested_timestamp else self.sql_select_all
            indexer_cursor.execute(select, time_range_json(from_timestamp, self.duration_seconds))
            result = indexer_cursor.fetchall()
            return self.prepare_data(result, start_of_range=from_timestamp)

    @staticmethod
    def prepare_data(parameters: list, **kwargs) -> list:
        # We usually have one-value returns, we need to merge it with corresponding date
        if len(parameters[0]) == 1:
            assert len(parameters) == 1, 'Only one value expected. Can\'t be sure that we need to add timestamp'
            computed_for = datetime.datetime.utcfromtimestamp(kwargs['start_of_range'])
            parameters = [(computed_for, parameters[0][0] or 0)]
        return [(computed_for.strftime('%Y-%m-%d'), data) for (computed_for, data) in parameters]
