from . import DAY_LEN_SECONDS, daily_start_of_range
from ..periodic_aggregations import PeriodicAggregations


class DailyDeployedContracts(PeriodicAggregations):
    @property
    def sql_create_table(self):
        return """
            CREATE TABLE IF NOT EXISTS daily_deployed_contracts
            (
                contract_code_sha256        text           NOT NULL,
                deployed_to_account_id      text           NOT NULL,
                deployed_by_receipt_id      text           PRIMARY KEY,
                -- It is important to store here not only day, but the exact timestamp
                -- Because there could be several deployments at the same day
                deployed_by_block_timestamp numeric(20, 0) NOT NULL
            );
            CREATE INDEX IF NOT EXISTS daily_deployed_contracts_timestamp_idx
                ON daily_deployed_contracts (deployed_by_block_timestamp);
            CREATE INDEX IF NOT EXISTS daily_deployed_contracts_sha256_idx
                ON daily_deployed_contracts (contract_code_sha256)
        """

    @property
    def sql_drop_table(self):
        return """
            DROP TABLE IF EXISTS daily_deployed_contracts
        """

    @property
    def sql_select(self):
        return """
            SELECT
                action_receipt_actions.args->>'code_sha256' as contract_code_sha256,
                receipts.receiver_account_id as deployed_to_account_id,
                receipts.receipt_id as deployed_by_receipt_id,
                receipts.included_in_block_timestamp as deployed_by_block_timestamp
            FROM action_receipt_actions
            JOIN receipts ON receipts.receipt_id = action_receipt_actions.receipt_id
            WHERE action_kind = 'DEPLOY_CONTRACT'
                AND receipts.included_in_block_timestamp >= %(from_timestamp)s
                AND receipts.included_in_block_timestamp < %(to_timestamp)s
            ORDER BY receipts.included_in_block_timestamp
        """

    @property
    def sql_insert(self):
        return """
            INSERT INTO daily_deployed_contracts VALUES %s 
            ON CONFLICT DO NOTHING
        """

    @property
    def duration_seconds(self):
        return DAY_LEN_SECONDS

    def start_of_range(self, timestamp: int) -> int:
        return daily_start_of_range(timestamp)

    @staticmethod
    def prepare_data(parameters: list, *, start_of_range=None, **kwargs) -> list:
        return parameters
