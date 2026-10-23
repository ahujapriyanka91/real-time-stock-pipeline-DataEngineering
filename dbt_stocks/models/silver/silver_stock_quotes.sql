-- Silver: clean, deduplicated quotes. One row per Kafka message.
-- Incremental: each run only processes rows loaded since the last run.
{{
    config(
        materialized='incremental',
        unique_key='quote_id',
        incremental_strategy='merge'
    )
}}

with source as (

    select * from {{ ref('bronze_stock_quotes') }}

    {% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}

),

deduped as (

    -- The consumer is at-least-once, so the same Kafka message can arrive twice.
    -- (partition, offset) uniquely identifies a message: keep the first copy.
    select *
    from source
    qualify row_number() over (
        partition by kafka_partition, kafka_offset
        order by loaded_at
    ) = 1

)

select
    kafka_partition || '-' || kafka_offset                          as quote_id,
    symbol,
    price,
    change_amount,
    change_pct,
    day_open,
    day_high,
    day_low,
    prev_close,
    quote_at_utc,
    convert_timezone('UTC', 'America/New_York', quote_at_utc)       as quote_at_ny,
    convert_timezone('UTC', 'America/New_York', quote_at_utc)::date as trading_date,
    ingested_at,
    loaded_at
from deduped
where price > 0
  and symbol is not null
