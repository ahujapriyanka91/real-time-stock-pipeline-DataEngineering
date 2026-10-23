-- An interval is invalid if high < low, or open/close fall outside the high-low range.
-- Returns the invalid rows; the test passes when it returns nothing.
select *
from {{ ref('gold_prices_5min') }}
where high_price < low_price
   or open_price  not between low_price and high_price
   or close_price not between low_price and high_price
