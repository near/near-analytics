import time
import typing

from datetime import date, datetime, timedelta


DAY_LEN_SECONDS = 86400
WEEK_LEN_SECONDS = DAY_LEN_SECONDS * 7
GENESIS_SECONDS = 1595350551


def daily_start_of_range(provided_timestamp: typing.Optional[int]) -> int:
    yesterday_timestamp = round(time.time()) - DAY_LEN_SECONDS
    timestamp = provided_timestamp or yesterday_timestamp
    return timestamp - timestamp % DAY_LEN_SECONDS


# TODO check if that works correctly, I'm not sure
def weekly_start_of_range(provided_timestamp: typing.Optional[int]) -> int:
    previous_week_timestamp = round(time.time()) - WEEK_LEN_SECONDS
    timestamp = provided_timestamp or previous_week_timestamp
    day: date = datetime.utcfromtimestamp(timestamp).date()
    monday: date = day - timedelta(days=day.weekday())
    seconds_since_epoch = (monday - datetime(1970, 1, 1).date()).total_seconds()
    return round(seconds_since_epoch)
