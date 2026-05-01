WITH base_traces AS (
  -- 1. 提取基础 Trace 数据
  SELECT
    transaction_hash,
    block_number,
    block_timestamp,
    from_address, -- 直接调用者 (可能是 Spender 合约)
    to_address,   -- 被调用的合约 (Token 地址)
    input,
    trace_address,
    status
  FROM
    `bigquery-public-data.crypto_ethereum.traces`
  WHERE
    block_timestamp >= '2024-12-01 00:00:00 UTC'
    AND block_timestamp < '2025-01-01 00:00:00 UTC'
    AND status = 1 -- 必须成功
    AND call_type = 'call' -- 只看直接调用，忽略 delegatecall (因为最终修改状态必须是 call)
    AND (STARTS_WITH(input, '0xd505accf') OR STARTS_WITH(input, '0x23b872dd'))
),

-- 2. 提取交易发起者 (Gas Payer)
base_transactions AS (
  SELECT
    `hash` AS tx_id,
    from_address AS original_submitter
  FROM
    `bigquery-public-data.crypto_ethereum.transactions`
  WHERE
    block_timestamp >= '2024-12-01 00:00:00 UTC'
    AND block_timestamp < '2025-01-01 00:00:00 UTC'
),

-- 3. 提取 Permit (标准 ERC-2612)
permits AS (
  SELECT 
    transaction_hash,
    block_number,
    block_timestamp,
    from_address AS direct_caller, -- 通常是 Spender
    to_address AS token_address,
    input AS permit_input,
    trace_address AS permit_trace
  FROM base_traces 
  WHERE STARTS_WITH(input, '0xd505accf')
),

-- 4. 提取 TransferFrom (改为数组聚合，避免 ANY_VALUE 丢数据)
transferfrom_agg AS (
  SELECT
    transaction_hash,
    to_address AS token_address,
    -- 将该交易下、该 Token 的所有 transferFrom 打包成数组
    ARRAY_AGG(STRUCT(input AS tf_input, trace_address AS tf_trace)) AS transfers
  FROM base_traces
  WHERE STARTS_WITH(input, '0x23b872dd')
  GROUP BY transaction_hash, token_address
)

-- 5. 输出结果
SELECT
    p.transaction_hash AS tx_hash,
    p.block_number,
    UNIX_SECONDS(p.block_timestamp) AS timestamp,
    
    tx.original_submitter,    -- 链上发起人 (EOA, 支付 Gas)
    p.direct_caller AS relayer, -- 调用 Permit 的人 (通常是 Spender 合约)
    p.token_address,
    
    p.permit_input,
    p.permit_trace,
    
    -- 如果有 TransferFrom，这里会是一个数组，Python 处理时更灵活
    t.transfers AS transfer_list 
FROM 
    permits AS p
LEFT JOIN 
    transferfrom_agg AS t
ON 
    p.transaction_hash = t.transaction_hash 
    AND p.token_address = t.token_address -- 确保是同一个 Token
JOIN 
    base_transactions AS tx
ON 
    p.transaction_hash = tx.tx_id

ORDER BY 
    timestamp ASC