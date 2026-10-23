-- Gold: How did the price move during the day?
--price summary per stock for every 5-minute interval (open/high/low/close).
-- Built from the quotes we received, grouped by New York market time.
with prices as (

    select
        symbol,
        price,
        quote_at_ny,
        time_slice(quote_at_ny, 5, 'MINUTE') as interval_start
    from {{ ref('silver_stock_quotes') }}

)

select
    symbol,
    interval_start,
    max_by(price, quote_at_ny)  as close_price,
    min_by(price, quote_at_ny)  as open_price,
    max(price)                  as high_price,
    min(price)                  as low_price,
    count(*)                    as num_quotes
from prices
group by symbol, interval_start
