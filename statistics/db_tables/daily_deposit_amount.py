import typing

from . import DAY_LEN_SECONDS, daily_start_of_range
from ..sql_statistics import SqlStatistics


class DailyDepositAmount(SqlStatistics):
    @property
    def sql_create_table(self):
        # For September 2021, the biggest value here is 10^34.
        # In Indexer, we store all the balances in numeric(45,0), including total_supply.
        # Assuming the users move not more than total_supply inside each second,
        # I suggest to use numeric(50, 0)
        return '''
            CREATE TABLE IF NOT EXISTS daily_deposit_amount
            (
                collected_for_day DATE PRIMARY KEY,
                deposit_amount    numeric(50, 0) NOT NULL
            )
        '''

    @property
    def sql_select(self):
        return '''
            SELECT SUM((action_receipt_actions.args->>'deposit')::numeric)
            FROM action_receipt_actions
            JOIN execution_outcomes ON execution_outcomes.receipt_id = action_receipt_actions.receipt_id
            JOIN receipts ON receipts.receipt_id = action_receipt_actions.receipt_id
            WHERE execution_outcomes.executed_in_block_timestamp > %(from_timestamp)s
                AND execution_outcomes.executed_in_block_timestamp < %(to_timestamp)s
                AND receipts.predecessor_account_id != 'system'
                AND action_receipt_actions.action_kind IN ('FUNCTION_CALL', 'TRANSFER')
                AND (action_receipt_actions.args->>'deposit')::numeric > 0
                AND execution_outcomes.status IN ('SUCCESS_VALUE', 'SUCCESS_RECEIPT_ID')
        '''

    @property
    def sql_insert(self):
        return '''
            INSERT INTO daily_deposit_amount VALUES (
                %(collected_for_day)s,
                %(result)s
            )
        '''

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, requested_statistics_timestamp: typing.Optional[int]) -> int:
        return daily_start_of_range(requested_statistics_timestamp)
