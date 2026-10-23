-- Gold: tells How did each stock do today.
-- Day open/high/low come from Finnhub's own daily stats on the last quote of the day.
-- Calculates daily return and intraday range, plus the number of quotes received for each stock today.
with last_quote_of_day as (

    select *
    from {{ ref('silver_stock_quotes') }}
    qualify row_number() over (
        partition by symbol, trading_date
        order by quote_at_utc desc, ingested_at desc
    ) = 1

),

quote_counts as (

    select symbol, trading_date, count(*) as num_quotes
    from {{ ref('silver_stock_quotes') }}
    group by symbol, trading_date

)

select
    q.symbol,
    s.company_name,
    s.sector,
    q.trading_date,
    q.day_open                                                   as open_price,
    q.day_high                                                   as high_price,
    q.day_low                                                    as low_price,
    q.price                                                      as close_price,
    q.prev_close,
    round((q.price - q.prev_close) / nullif(q.prev_close, 0) * 100, 2)       as daily_return_pct,
    round((q.day_high - q.day_low) / nullif(q.day_open, 0) * 100, 2)         as intraday_range_pct,
    c.num_quotes
from last_quote_of_day q
join quote_counts c
  on q.symbol = c.symbol and q.trading_date = c.trading_date
left join {{ ref('symbols') }} s
  on q.symbol = s.symbol
