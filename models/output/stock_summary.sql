{{ config(materialized='table') }}

WITH ma_stats AS (
    SELECT 
        symbol,
        ROUND(AVG(ma_14), 2) AS avg_ma_14,
        ROUND(AVG(rsi_14), 2) AS avg_rsi_14,
        MIN(date) AS start_date_ma,
        MAX(date) AS end_date_ma
    FROM {{ ref('ma_rsi') }}
    GROUP BY symbol
),

momentum_stats AS (
    SELECT 
        symbol,
        ROUND(AVG(daily_return), 2) AS avg_daily_return,
        ROUND(MAX(daily_return), 2) AS max_daily_return,
        ROUND(MIN(daily_return), 2) AS min_daily_return,
        ROUND(AVG(momentum_3d), 2) AS avg_momentum_3d,
        ROUND(AVG(momentum_7d), 2) AS avg_momentum_7d,
        MIN(date) AS start_date_momentum,
        MAX(date) AS end_date_momentum
    FROM {{ ref('daily_momentum') }}
    GROUP BY symbol
)

SELECT
    m.symbol,
    m.avg_ma_14,
    m.avg_rsi_14,
    d.avg_daily_return,
    d.max_daily_return,
    d.min_daily_return,
    d.avg_momentum_3d,
    d.avg_momentum_7d,
    LEAST(m.start_date_ma, d.start_date_momentum) AS start_date,
    GREATEST(m.end_date_ma, d.end_date_momentum) AS end_date,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM ma_stats m
JOIN momentum_stats d ON m.symbol = d.symbol
