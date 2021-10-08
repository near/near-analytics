from ..sql_aggregations import SqlAggregations


# Since we don't have the logic that will check whether we collected all previous days,
# AND this aggregation is required for other statistics,
# (read: if deployed_contracts are not filled properly, others could be computed in a wrong way)
# We decided to recompute this metric fully each time. That gives us confidence in our numbers.
class DeployedContracts(SqlAggregations):
    @property
    def sql_create_table(self):
        return '''
            CREATE TABLE IF NOT EXISTS deployed_contracts
            (
                code_sha256                      text PRIMARY KEY,
                contract_id                      text           NOT NULL,
                first_created_by_receipt_id      text           NOT NULL,
                first_created_by_block_timestamp numeric(20, 0) NOT NULL
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
                receipts.receipt_id as first_created_by_receipt_id,
                receipts.included_in_block_timestamp as first_created_by_block_timestamp
            FROM action_receipt_actions
            JOIN receipts ON receipts.receipt_id = action_receipt_actions.receipt_id
            WHERE action_kind = 'DEPLOY_CONTRACT'
            ORDER BY receipts.included_in_block_timestamp
        '''

    @property
    def sql_insert(self):
        return '''
            INSERT INTO deployed_contracts VALUES %s 
            ON CONFLICT DO NOTHING
        '''
