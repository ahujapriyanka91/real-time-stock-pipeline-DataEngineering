-- Bronze: turn the raw JSON into typed columns. No filtering yet.
select
    record:kafka_partition::int                     as kafka_partition,
    record:kafka_offset::int                        as kafka_offset,
    record:symbol::string                           as symbol,
    record:price::number(18, 4)                     as price,
    record:change::number(18, 4)                    as change_amount,
    record:change_pct::number(18, 4)                as change_pct,
    record:open::number(18, 4)                      as day_open,
    record:high::number(18, 4)                      as day_high,
    record:low::number(18, 4)                       as day_low,
    record:prev_close::number(18, 4)                as prev_close,
    to_timestamp_ntz(record:quote_ts::int)          as quote_at_utc,      -- last trade time from Finnhub
    record:ingested_at::timestamp_tz                as ingested_at,       -- when our producer fetched it
    source_file,
    loaded_at
from {{ source('raw', 'stock_quotes') }}
