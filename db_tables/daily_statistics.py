from dataclasses import dataclass


@dataclass
class DailyStatistic:
    computed_for_timestamp: int
    transactions_count: int
    teragas_used: int
    deposit_amount: int
    new_accounts_count: int
    deleted_accounts_count: int
    active_accounts_count: int
    new_contracts_count: int
    new_unique_contracts_count: int
    active_contracts_count: int

    def get_insert_string(self) -> str:
        return f'''
            INSERT INTO daily_statistics VALUES (
                {self.computed_for_timestamp},
                {self.transactions_count},
                {self.teragas_used},
                {self.deposit_amount},
                {self.new_accounts_count},
                {self.deleted_accounts_count},
                {self.active_accounts_count},
                {self.new_contracts_count},
                {self.new_unique_contracts_count},
                {self.active_contracts_count}
            );
        '''


def transactions_count(from_timestamp, to_timestamp) -> str:
    return f'''
        SELECT COUNT(*) FROM transactions
        WHERE block_timestamp > {from_timestamp} AND block_timestamp < {to_timestamp}
    '''


def teragas_used(from_timestamp, to_timestamp) -> str:
    return f'''
        SELECT DIV(SUM(chunks.gas_used), 1000000000000)
        FROM blocks
        JOIN chunks ON chunks.included_in_block_hash = blocks.block_hash
        WHERE blocks.block_timestamp > {from_timestamp}
            AND blocks.block_timestamp < {to_timestamp}
    '''


def deposit_amount(from_timestamp, to_timestamp) -> str:
    return f'''
        SELECT SUM((action_receipt_actions.args->>'deposit')::numeric)
        FROM action_receipt_actions
        JOIN execution_outcomes ON execution_outcomes.receipt_id = action_receipt_actions.receipt_id
        JOIN receipts ON receipts.receipt_id = action_receipt_actions.receipt_id
        WHERE execution_outcomes.executed_in_block_timestamp > {from_timestamp}
            AND execution_outcomes.executed_in_block_timestamp < {to_timestamp}
            AND receipts.predecessor_account_id != 'system'
            AND action_receipt_actions.action_kind IN ('FUNCTION_CALL', 'TRANSFER')
            AND (action_receipt_actions.args->>'deposit')::numeric > 0
            AND execution_outcomes.status IN ('SUCCESS_VALUE', 'SUCCESS_RECEIPT_ID')
    '''


def new_accounts_count(from_timestamp, to_timestamp) -> str:
    return f'''
        SELECT COUNT(created_by_receipt_id)
        FROM accounts
        JOIN receipts ON receipts.receipt_id = accounts.created_by_receipt_id
        WHERE receipts.included_in_block_timestamp > {from_timestamp}
            AND receipts.included_in_block_timestamp < {to_timestamp}
    '''


def deleted_accounts_count(from_timestamp, to_timestamp) -> str:
    return f'''
        SELECT COUNT(accounts.deleted_by_receipt_id)
        FROM accounts
        JOIN receipts ON receipts.receipt_id = accounts.deleted_by_receipt_id
        WHERE receipts.included_in_block_timestamp > {from_timestamp}
            AND receipts.included_in_block_timestamp < {to_timestamp}
    '''


def active_accounts_count(from_timestamp, to_timestamp) -> str:
    return f'''
        SELECT COUNT(DISTINCT transactions.signer_account_id)
        FROM transactions
        JOIN execution_outcomes ON execution_outcomes.receipt_id = transactions.converted_into_receipt_id
        WHERE transactions.block_timestamp > {from_timestamp}
            AND transactions.block_timestamp < {to_timestamp}
            AND execution_outcomes.status IN ('SUCCESS_VALUE', 'SUCCESS_RECEIPT_ID')
    '''


def new_contracts_count(from_timestamp, to_timestamp) -> str:
    return f'''
        SELECT COUNT(DISTINCT receipts.receiver_account_id)
        FROM action_receipt_actions
        JOIN receipts ON receipts.receipt_id = action_receipt_actions.receipt_id
        WHERE receipts.included_in_block_timestamp > {from_timestamp}
            AND receipts.included_in_block_timestamp < {to_timestamp}
            AND action_receipt_actions.action_kind = 'DEPLOY_CONTRACT'
    '''


def new_unique_contracts_count(from_timestamp, to_timestamp) -> str:
    return f'''
        SELECT COUNT(DISTINCT args->>'code_sha256')
        FROM action_receipt_actions
        JOIN receipts ON receipts.receipt_id = action_receipt_actions.receipt_id
        WHERE receipts.included_in_block_timestamp > {from_timestamp}
            AND receipts.included_in_block_timestamp < {to_timestamp}
            AND action_kind = 'DEPLOY_CONTRACT'
    '''


def active_contracts_count(from_timestamp, to_timestamp) -> str:
    return f'''
        SELECT COUNT(DISTINCT execution_outcomes.executor_account_id)
        FROM action_receipt_actions
        JOIN execution_outcomes ON execution_outcomes.receipt_id = action_receipt_actions.receipt_id
        WHERE execution_outcomes.executed_in_block_timestamp > {from_timestamp}
            AND execution_outcomes.executed_in_block_timestamp < {to_timestamp}
            AND action_receipt_actions.action_kind = 'FUNCTION_CALL'
            AND execution_outcomes.status IN ('SUCCESS_VALUE', 'SUCCESS_RECEIPT_ID')
    '''
