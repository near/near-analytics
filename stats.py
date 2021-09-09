import argparse
import os
import time

from dotenv import load_dotenv
from constants import DAY_LEN
from db_utils import collect_data_for_timestamp, save_statistics


def find_day_start_for_timestamp(timestamp):
    return timestamp - timestamp % DAY_LEN


def handle_timestamp(indexer_db, analytics_db, timestamp):
    data = collect_data_for_timestamp(indexer_db, timestamp)
    save_statistics(analytics_db, data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compute statistics for mainnet')
    parser.add_argument('-t', '--timestamp', type=int,
                        help='The day to compute. Default today')
    args = parser.parse_args()

    provided_timestamp = args.timestamp or time.time_ns()
    compute_for_timestamp = find_day_start_for_timestamp(provided_timestamp)

    load_dotenv()
    ANALYTICS_DATABASE_URL = os.getenv('WRITE_DATABASE_URL')
    INDEXER_DATABASE_URL = os.getenv('READ_DATABASE_URL')

    handle_timestamp(INDEXER_DATABASE_URL, ANALYTICS_DATABASE_URL, compute_for_timestamp)
