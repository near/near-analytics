# https://github.com/near/near-explorer/blob/13ca8c0487043c69b1f770908005506d0c9375eb/backend/src/db-utils.js#L261-L561

# const queryActiveAccountsCountAggregatedByWeek = async () => {
#   return await queryRows(
#     [
#       `SELECT
#         DATE_TRUNC('week', TO_TIMESTAMP(DIV(transactions.block_timestamp, 1000*1000*1000))) AS date,
#         COUNT(DISTINCT transactions.signer_account_id) AS active_accounts_count_by_week
#       FROM transactions
#       JOIN execution_outcomes ON execution_outcomes.receipt_id = transactions.converted_into_receipt_id
#       WHERE execution_outcomes.status IN ('SUCCESS_VALUE', 'SUCCESS_RECEIPT_ID')
#       AND transactions.block_timestamp < ((CAST(EXTRACT(EPOCH FROM DATE_TRUNC('week', NOW())) AS bigint)) * 1000 * 1000 * 1000)
#       GROUP BY date
#       ORDER BY date`,
#     ],
#     { dataSource: DS_INDEXER_BACKEND }
#   );
# };
#
# const queryActiveAccountsList = async () => {
#   return await queryRows(
#     [
#       `SELECT
#         signer_account_id,
#         COUNT(*) AS transactions_count
#       FROM transactions
#       WHERE transactions.block_timestamp >= (CAST(EXTRACT(EPOCH FROM DATE_TRUNC('day', NOW() - INTERVAL '2 week')) AS bigint) * 1000 * 1000 * 1000)
#       AND transactions.block_timestamp < (CAST(EXTRACT(EPOCH FROM DATE_TRUNC('day', NOW())) AS bigint) * 1000 * 1000 * 1000)
#       GROUP BY signer_account_id
#       ORDER BY transactions_count DESC
#       LIMIT 10`,
#     ],
#     { dataSource: DS_INDEXER_BACKEND }
#   );
# };
#

#
# const queryActiveContractsList = async () => {
#   return await queryRows(
#     [
#       `SELECT
#         receiver_account_id,
#         COUNT(receipts.receipt_id) AS receipts_count
#       FROM action_receipt_actions
#       JOIN receipts ON receipts.receipt_id = action_receipt_actions.receipt_id
#       WHERE action_receipt_actions.action_kind = 'FUNCTION_CALL'
#       AND receipts.included_in_block_timestamp >= (CAST(EXTRACT(EPOCH FROM DATE_TRUNC('day', NOW() - INTERVAL '2 week')) AS bigint) * 1000 * 1000 * 1000)
#       AND receipts.included_in_block_timestamp < (CAST(EXTRACT(EPOCH FROM DATE_TRUNC('day', NOW())) AS bigint) * 1000 * 1000 * 1000)
#       GROUP BY receiver_account_id
#       ORDER BY receipts_count DESC
#       LIMIT 10`,
#     ],
#     { dataSource: DS_INDEXER_BACKEND }
#   );
# };
#
# // query for partners
# const queryPartnerTotalTransactions = async () => {
#   return await queryRows(
#     [
#       `SELECT
#         receiver_account_id,
#         COUNT(*) AS transactions_count
#       FROM transactions
#       WHERE receiver_account_id IN (:partner_list)
#       GROUP BY receiver_account_id
#       ORDER BY transactions_count DESC
#       `,
#       { partner_list: PARTNER_LIST },
#     ],
#     { dataSource: DS_INDEXER_BACKEND }
#   );
# };
#
# const queryPartnerFirstThreeMonthTransactions = async () => {
#   let partnerList = Array(PARTNER_LIST.length);
#   for (let i = 0; i < PARTNER_LIST.length; i++) {
#     let result = await querySingleRow(
#       [
#         `SELECT
#           :partner AS receiver_account_id,
#           COUNT(*) AS transactions_count
#         FROM transactions
#         WHERE receiver_account_id = :partner
#         AND TO_TIMESTAMP(block_timestamp / 1000000000) < (
#           SELECT
#             (TO_TIMESTAMP(block_timestamp / 1000000000) + INTERVAL '3 month')
#           FROM transactions
#           WHERE receiver_account_id = :partner
#           ORDER BY block_timestamp
#           LIMIT 1)
#       `,
#         { partner: PARTNER_LIST[i] },
#       ],
#       { dataSource: DS_INDEXER_BACKEND }
#     );
#     partnerList[i] = result;
#   }
#   return partnerList;
# };
#
# const queryPartnerUniqueUserAmount = async () => {
#   return await queryRows(
#     [
#       `SELECT
#         receiver_account_id,
#         COUNT(DISTINCT predecessor_account_id) AS user_amount
#       FROM receipts
#       WHERE receiver_account_id IN (:partner_list)
#       GROUP BY receiver_account_id
#       ORDER BY user_amount DESC`,
#       { partner_list: PARTNER_LIST },
#     ],
#     { dataSource: DS_INDEXER_BACKEND }
#   );
# };
