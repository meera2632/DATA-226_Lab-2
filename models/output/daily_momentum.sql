{{ config(materialized='table') }}

WITH base AS (
    SELECT
        date,
        symbol,
        close
    FROM {{ ref('stock_price') }}
),

returns AS (
    SELECT
        *,
        ROUND(
            100 * (close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) /
            NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0),
            2
        ) AS daily_return,
        
        close - LAG(close, 3) OVER (PARTITION BY symbol ORDER BY date) AS momentum_3d,
        close - LAG(close, 7) OVER (PARTITION BY symbol ORDER BY date) AS momentum_7d
    FROM base
)

SELECT
    date,
    symbol,
    close,
    daily_return,
    momentum_3d,
    momentum_7d
FROM returns
ORDER BY symbol, date
