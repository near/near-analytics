import time
import typing

from datetime import date, datetime, timedelta

DAY_LEN_SECONDS = 86400
WEEK_LEN_SECONDS = DAY_LEN_SECONDS * 7


def query_genesis_timestamp(indexer_connection) -> int:
    select_genesis_timestamp = '''
                SELECT DIV(block_timestamp, 1000 * 1000 * 1000)
                FROM blocks
                ORDER BY block_timestamp
                LIMIT 1
            '''
    with indexer_connection.cursor() as indexer_cursor:
        indexer_cursor.execute(select_genesis_timestamp)
        return indexer_cursor.fetchone()[0]

def daily_start_of_range(provided_timestamp: typing.Optional[int]) -> int:
    yesterday_timestamp = round(time.time()) - DAY_LEN_SECONDS
    timestamp = provided_timestamp or yesterday_timestamp
    return timestamp - timestamp % DAY_LEN_SECONDS


def weekly_start_of_range(provided_timestamp: typing.Optional[int]) -> int:
    previous_week_timestamp = round(time.time()) - WEEK_LEN_SECONDS
    timestamp = provided_timestamp or previous_week_timestamp
    day: date = datetime.utcfromtimestamp(timestamp).date()
    monday: date = day - timedelta(days=day.weekday())
    seconds_since_epoch = (monday - datetime(1970, 1, 1).date()).total_seconds()
    return round(seconds_since_epoch)


def to_nanos(timestamp_seconds):
    return timestamp_seconds * 1000 * 1000 * 1000


def time_range_json(from_timestamp, duration):
    return {
        'from_timestamp': to_nanos(from_timestamp),
        'to_timestamp': to_nanos(from_timestamp + duration)
    }


def time_json(timestamp):
    return {
        'timestamp': to_nanos(timestamp)
    }
