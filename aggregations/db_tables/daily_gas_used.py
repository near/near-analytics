import typing

from . import DAY_LEN_SECONDS, daily_start_of_range
from ..periodic_aggregations import PeriodicAggregations


class DailyGasUsed(PeriodicAggregations):
    @property
    def sql_create_table(self):
        # In Indexer, we store `chunks.gas_used` in numeric(20,0).
        # We produce the block each second (10^5).
        # We plan to use several chunks, let's say max chunks count is 10^3
        # 28 sounds weird, so I suggest numeric(30, 0)
        return '''
            CREATE TABLE IF NOT EXISTS daily_gas_used
            (
                collected_for_day DATE PRIMARY KEY,
                gas_used          numeric(30, 0) NOT NULL
            )
        '''

    @property
    def sql_drop_table(self):
        return '''
            DROP TABLE IF EXISTS daily_gas_used
        '''

    @property
    def sql_select(self):
        return '''
            SELECT SUM(chunks.gas_used)
            FROM blocks
            JOIN chunks ON chunks.included_in_block_hash = blocks.block_hash
            WHERE blocks.block_timestamp >= %(from_timestamp)s
                AND blocks.block_timestamp < %(to_timestamp)s
        '''

    @property
    def sql_select_all(self):
        return '''
            SELECT
                DATE_TRUNC('day', TO_TIMESTAMP(DIV(blocks.block_timestamp, 1000 * 1000 * 1000))) AS date,
                SUM(chunks.gas_used) AS gas_used_by_date
            FROM blocks
            JOIN chunks ON chunks.included_in_block_hash = blocks.block_hash
            WHERE blocks.block_timestamp < (CAST(EXTRACT(EPOCH FROM DATE_TRUNC('day', NOW())) AS bigint) * 1000 * 1000 * 1000)
            GROUP BY date
        '''

    @property
    def sql_insert(self):
        return '''
            INSERT INTO daily_gas_used VALUES %s
            ON CONFLICT DO NOTHING
        '''

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, requested_statistics_timestamp: typing.Optional[int]) -> int:
        return daily_start_of_range(requested_statistics_timestamp)
