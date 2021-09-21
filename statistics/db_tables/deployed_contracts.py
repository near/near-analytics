from ..aggregations import Aggregations


class DeployedContracts(Aggregations):
    @property
    def sql_create_table(self):
        return '''
            CREATE TABLE IF NOT EXISTS deployed_contracts
            (
                code_sha256                text PRIMARY KEY,
                contract_id                text           NOT NULL,
                created_by_receipt_id      text           NOT NULL,
                created_by_block_timestamp numeric(20, 0) NOT NULL
            )
        '''

    @property
    def sql_drop_table(self):
        return '''
            DROP TABLE IF EXISTS deployed_contracts
        '''

    @property
    def sql_select(self):
        return '''
            SELECT
                action_receipt_actions.args->>'code_sha256' as code_sha256,
                receipts.receiver_account_id as contract_id,
                receipts.receipt_id as created_by_receipt_id,
                receipts.included_in_block_timestamp as created_by_block_timestamp
            FROM action_receipt_actions
            JOIN receipts ON receipts.receipt_id = action_receipt_actions.receipt_id
            WHERE receipts.included_in_block_timestamp >= %(timestamp)s
            AND action_kind = 'DEPLOY_CONTRACT'
            ORDER BY receipts.included_in_block_timestamp
        '''

    @property
    def sql_select_all(self):
        return '''
            SELECT
                action_receipt_actions.args->>'code_sha256' as code_sha256,
                receipts.receiver_account_id as contract_id,
                receipts.receipt_id as created_by_receipt_id,
                receipts.included_in_block_timestamp as created_by_block_timestamp
            FROM action_receipt_actions
            JOIN receipts ON receipts.receipt_id = action_receipt_actions.receipt_id
            AND action_kind = 'DEPLOY_CONTRACT'
            ORDER BY receipts.included_in_block_timestamp
        '''

    @property
    def sql_insert(self):
        return '''
            INSERT INTO deployed_contracts VALUES %s 
            ON CONFLICT DO NOTHING
        '''
