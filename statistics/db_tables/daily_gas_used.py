import typing

from . import DAY_LEN_SECONDS, daily_start_of_range
from ..sql_statistics import SqlStatistics


class DailyGasUsed(SqlStatistics):
    @property
    def sql_create_table(self):
        # At the moment of September 2021,
        # we have average 10^13, max 10^15 gas_used per block (read as: per second)
        # For today, worst case gives ~10^20 gas_used per day.
        # Decided to double this assumption, so we have numeric(40, 0)
        return '''
            CREATE TABLE IF NOT EXISTS daily_gas_used
            (
                collected_for_day DATE PRIMARY KEY,
                gas_used          numeric(40, 0) NOT NULL
            )
        '''

    @property
    def sql_select(self):
        return '''
            SELECT SUM(chunks.gas_used)
            FROM blocks
            JOIN chunks ON chunks.included_in_block_hash = blocks.block_hash
            WHERE blocks.block_timestamp > %(from_timestamp)s
                AND blocks.block_timestamp < %(to_timestamp)s
        '''

    @property
    def sql_insert(self):
        return '''
            INSERT INTO daily_gas_used VALUES (
                %(collected_for_day)s,
                %(result)s
            )
        '''

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, requested_statistics_timestamp: typing.Optional[int]) -> int:
        return daily_start_of_range(requested_statistics_timestamp)
