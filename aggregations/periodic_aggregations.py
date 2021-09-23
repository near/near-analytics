import abc
import datetime
import typing

from .sql_aggregations import SqlAggregations
from .db_tables import time_range_json


class PeriodicAggregations(SqlAggregations):
    @abc.abstractmethod
    def start_of_range(self, requested_statistics_timestamp: typing.Optional[int]) -> int:
        pass

    @property
    @abc.abstractmethod
    def duration_seconds(self) -> int:
        pass

    # requested_timestamp will be rounded to the start of the day, week (Monday), month, etc.
    def collect(self, requested_timestamp: typing.Optional[int]) -> list:
        from_timestamp = self.start_of_range(requested_timestamp)
        if not self.is_indexer_ready(from_timestamp + self.duration_seconds):
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

    def is_indexer_ready(self, needed_timestamp):
        select_latest_timestamp = '''
            SELECT DIV(block_timestamp, 1000 * 1000 * 1000)
            FROM blocks
            ORDER BY block_timestamp DESC
            LIMIT 1
        '''
        with self.indexer_connection.cursor() as indexer_cursor:
            indexer_cursor.execute(select_latest_timestamp)
            latest_timestamp = indexer_cursor.fetchone()[0]
            return latest_timestamp >= needed_timestamp
