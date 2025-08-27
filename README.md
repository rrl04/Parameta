# Rolling Standard Deviation & Rates Conversion Pipelines

This repository contains two independent Python pipelines:

1. **rates.py** – merges price data with spot FX rates and currency conversion rules, producing converted prices.
2. **stdev.py** – computes 20-period rolling standard deviations of bid/mid/ask prices, but only when the last 20 rows are **hourly contiguous** (no missing hours).

---

## 📂 Project Structure

