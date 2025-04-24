{% snapshot stock_price_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='date || \'-\' || symbol',
    strategy='check',
    check_cols=['open', 'high', 'low', 'close', 'volume'],
    invalidate_hard_deletes=True
  )
}}

SELECT * FROM {{ source('raw', 'stock_prices') }}

{% endsnapshot %}
