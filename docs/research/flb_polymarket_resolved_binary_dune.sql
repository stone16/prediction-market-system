-- Strict H1 FLB warehouse export for scripts/flb_data_feasibility.py.
--
-- Source tables are Dune curated Polymarket Polygon tables:
--   polymarket_polygon.market_details
--   polymarket_polygon.market_prices_hourly
--   polymarket_polygon.market_trades
--
-- Output columns must stay aligned with WAREHOUSE_REQUIRED_COLUMNS in
-- scripts/flb_data_feasibility.py.  The export is one row per resolved binary
-- market, using the YES token price at or before the fixed entry horizon.
--
-- The liquidity column is a launch-safe local trade-volume proxy over the
-- seven days before the entry timestamp.  Runtime FLB calibration does not use
-- this value for scoring; it is retained for audit/reporting compatibility.

WITH resolved_binary_markets AS (
    SELECT
        lower(regexp_replace(condition_id, '^0x', '')) AS condition_id_hex,
        max(question) AS question,
        max(coalesce(tags, 'uncategorized')) AS tags,
        max(resolved_on_timestamp) AS resolved_at,
        max(CASE WHEN upper(token_outcome) = 'YES' THEN token_id END) AS yes_token_id,
        max(CASE WHEN upper(token_outcome) = 'NO' THEN token_id END) AS no_token_id,
        max(lower(trim(outcome))) AS outcome_label
    FROM polymarket_polygon.market_details
    WHERE closed = 'true'
      AND resolved_on_timestamp IS NOT NULL
      AND upper(token_outcome) IN ('YES', 'NO')
    GROUP BY condition_id
    HAVING count(DISTINCT upper(token_outcome)) = 2
       AND count(DISTINCT lower(trim(outcome))) = 1
       AND max(CASE WHEN upper(token_outcome) = 'YES' THEN token_id END) IS NOT NULL
       AND max(CASE WHEN upper(token_outcome) = 'NO' THEN token_id END) IS NOT NULL
       AND max(lower(trim(outcome))) IN ('yes', 'no')
),
entry_price_candidates AS (
    SELECT
        d.condition_id_hex,
        p.hour AS entry_timestamp,
        p.price AS entry_yes_price,
        row_number() OVER (
            PARTITION BY d.condition_id_hex
            ORDER BY p.hour DESC
        ) AS row_number
    FROM resolved_binary_markets d
    JOIN polymarket_polygon.market_prices_hourly p
      ON p.condition_id = from_hex(d.condition_id_hex)
     AND p.token_id = d.yes_token_id
     AND p.hour <= date_add('hour', -24, d.resolved_at)
    WHERE p.price > 0
      AND p.price < 1
),
entry_prices AS (
    SELECT
        condition_id_hex,
        entry_timestamp,
        entry_yes_price
    FROM entry_price_candidates
    WHERE row_number = 1
),
volume_by_market AS (
    SELECT
        d.condition_id_hex,
        sum(t.amount) AS volume
    FROM resolved_binary_markets d
    JOIN polymarket_polygon.market_trades t
      ON t.condition_id = from_hex(d.condition_id_hex)
     AND t.block_time < d.resolved_at
     AND t.block_time >= TIMESTAMP '2020-01-01'
    GROUP BY d.condition_id_hex
),
entry_liquidity_proxy AS (
    SELECT
        d.condition_id_hex,
        sum(t.amount) AS liquidity
    FROM resolved_binary_markets d
    JOIN entry_prices e
      ON e.condition_id_hex = d.condition_id_hex
    JOIN polymarket_polygon.market_trades t
      ON t.condition_id = from_hex(d.condition_id_hex)
     AND t.block_time >= date_add('day', -7, e.entry_timestamp)
     AND t.block_time < e.entry_timestamp
     AND t.block_time >= TIMESTAMP '2020-01-01'
    GROUP BY d.condition_id_hex
)
SELECT
    '0x' || d.condition_id_hex AS market_id,
    d.question,
    CAST(e.entry_yes_price AS DOUBLE) AS entry_yes_price,
    CASE WHEN d.outcome_label = 'yes' THEN 1 ELSE 0 END AS yes_payout,
    CASE WHEN d.outcome_label = 'no' THEN 1 ELSE 0 END AS no_payout,
    CAST(coalesce(v.volume, 0) AS DOUBLE) AS volume,
    CAST(coalesce(l.liquidity, 0) AS DOUBLE) AS liquidity,
    CAST(e.entry_timestamp AS VARCHAR) AS entry_timestamp,
    CAST(d.resolved_at AS VARCHAR) AS resolved_at,
    coalesce(nullif(trim(d.tags), ''), 'uncategorized') AS category
FROM resolved_binary_markets d
JOIN entry_prices e
  ON e.condition_id_hex = d.condition_id_hex
LEFT JOIN volume_by_market v
  ON v.condition_id_hex = d.condition_id_hex
LEFT JOIN entry_liquidity_proxy l
  ON l.condition_id_hex = d.condition_id_hex
ORDER BY d.resolved_at DESC, d.condition_id_hex;
