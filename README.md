# Rolling Standard Deviation & Rates Conversion Pipelines

This repository contains two independent Python pipelines:

1. **rates.py** – merges price data with spot FX rates and currency conversion rules, producing converted prices.

bash
python rates.py \
    --ccy rates_ccy_data.csv \
    --spot rates_spot_rate_data.parq.gzip \
    --price rates_price_data.parq.gzip \
    --output converted_prices.csv \
    --start "2021-11-20 00:00:00" \
    --end "2021-11-23 09:00:00"

Arguments:
--ccy (default: rates_ccy_data.csv) → Currency conversion rules
--spot (default: rates_spot_rate_data.parq.gzip) → Spot FX rates
--price (default: rates_price_data.parq.gzip) → Raw prices
--output (default: converted_prices.csv) → Output file under results/
--start / --end (optional) → Datetime filters

2. **stdev.py** – computes 20-period rolling standard deviations of bid/mid/ask prices, but only when the last 20 rows are **hourly contiguous** (no missing hours).

bash
python stdev.py \
    --input stdev_price_data.parq.gzip \
    --output rolling_stdev.csv \
    --start "2021-11-20 00:00:00" \
    --end "2021-11-23 09:00:00"
    
Arguments:
--input (default: stdev_price_data.parq.gzip) → Price input file
--output (default: rolling_stdev.csv) → Output file under results/
--start (optional) → Start datetime filter
--end (optional) → End datetime filter


