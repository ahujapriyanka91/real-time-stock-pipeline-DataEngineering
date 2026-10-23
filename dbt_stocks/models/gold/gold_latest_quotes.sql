-- Gold: What's the price right now? 
--latest price per stock, with company info. Feeds the KPI cards.
with latest as (

    select *
    from {{ ref('silver_stock_quotes') }}
    qualify row_number() over (partition by symbol order by ingested_at desc) = 1

)

select
    l.symbol,
    s.company_name,
    s.sector,
    l.price,
    l.change_amount,
    l.change_pct,
    l.day_open,
    l.day_high,
    l.day_low,
    l.prev_close,
    l.quote_at_ny,
    l.ingested_at
from latest l
left join {{ ref('symbols') }} s on l.symbol = s.symbol
