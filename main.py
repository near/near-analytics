import datetime
import os
import time

from dotenv import load_dotenv

from constants import GENESIS, DAY_LEN
from stats import handle_timestamp


current_timestamp = GENESIS
load_dotenv()
ANALYTICS_DATABASE_URL = os.getenv('WRITE_DATABASE_URL')
INDEXER_DATABASE_URL = os.getenv('READ_DATABASE_URL')

while current_timestamp < time.time_ns():
    print(datetime.date.fromtimestamp(current_timestamp / 1000 / 1000 / 1000))
    handle_timestamp(INDEXER_DATABASE_URL, ANALYTICS_DATABASE_URL, current_timestamp)
    current_timestamp += DAY_LEN
