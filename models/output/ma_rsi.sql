{{ config(materialized='table') }}

WITH base AS (
    SELECT
        date,
        symbol,
        close,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date) AS row_num
    FROM {{ ref('stock_price') }}
),

price_diff AS (
    SELECT
        b.*,
        close - LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS price_change
    FROM base b
),

rsi_calc AS (
    SELECT
        *,
        CASE WHEN price_change > 0 THEN price_change ELSE 0 END AS gain,
        CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END AS loss
    FROM price_diff
),

rolling_avg AS (
    SELECT
        *,
        AVG(gain) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
        AVG(loss) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
    FROM rsi_calc
),

final AS (
    SELECT
        date,
        symbol,
        close,
        ROUND(AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW), 2) AS ma_14,
        CASE
            WHEN avg_loss = 0 THEN 100
            ELSE ROUND(100 - (100 / (1 + avg_gain / NULLIF(avg_loss, 0))), 2)
        END AS rsi_14
    FROM rolling_avg
)

SELECT * FROM final
