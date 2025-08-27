# rates.py
import pandas as pd
from pathlib import Path
import argparse


class RatesProcessor:
    def __init__(self, data_dir: str = None, output_dir: str = None):
        '''
        Initialize processor with data and output directories.
        Defaults to <repo_root>/data and <repo_root>/results.
        '''
        repo_root = Path(__file__).resolve().parents[1]  # rates_test/
        self.data_dir = Path(data_dir) if data_dir else repo_root / 'data'
        self.output_dir = Path(output_dir) if output_dir else repo_root / 'results'
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _load(self, filename: str) -> pd.DataFrame:
        '''
        Load CSV or Parquet file into a pandas DataFrame.
        Ensures timestamp is converted to datetime.
        '''
        path = self.data_dir / filename
        if not path.exists():
            raise FileNotFoundError(f'File not found: {path}')

        if filename.endswith('.csv'):
            df = pd.read_csv(path)
        else:  # parquet files (.parq.gzip)
            df = pd.read_parquet(path, engine='pyarrow')

        # ensure timestamp exists and is datetime
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

        return df

    def process(
        self,
        ccy_file: str = "rates_ccy_data.csv",
        spot_file: str = "rates_spot_rate_data.parq.gzip",
        price_file: str = "rates_price_data.parq.gzip",
        output_file: str = "converted_prices.csv",
        start: str = None,
        end: str = None,
    ) -> pd.DataFrame:
        '''
        Main pipeline: 
          - load datasets
          - merge spot + ccy rules into price
          - compute converted prices
          - filter by optional [start, end] window
          - save output
        '''
        # Load datasets
        ccy = self._load(ccy_file)
        spot = self._load(spot_file)
        price = self._load(price_file)

        # Ensure sorting by merge keys (needed for merge_asof)
        price = price.sort_values(['timestamp', 'ccy_pair'], kind='mergesort').reset_index(drop=True)
        spot = spot.sort_values(['timestamp', 'ccy_pair'], kind='mergesort').reset_index(drop=True)

        # Merge spot into price (asof within 1 hour)
        merged = pd.merge_asof(
            price,
            spot,
            by='ccy_pair',
            left_on='timestamp',
            right_on='timestamp',
            direction='backward',
            tolerance=pd.Timedelta('1h'),
        )
        
        # Merge in ccy conversion rules
        merged = merged.merge(ccy, on='ccy_pair', how='left')
        
        # after merging in ccy conversion rules
        if 'convert_price' in merged.columns:
            merged['convert_price'] = merged['convert_price'].astype(bool)

        # ============================
        # Vectorized new_price calc
        # ============================
        merged['new_price'] = pd.NA  
        has_spot = merged['spot_mid_rate'].notna()

        needs_convert = has_spot & merged['convert_price']
        no_convert = has_spot & ~merged['convert_price']

        merged.loc[no_convert, 'new_price'] = merged.loc[no_convert, 'price']
        merged.loc[needs_convert, 'new_price'] = (
            merged.loc[needs_convert, 'price'] / merged.loc[needs_convert, 'conversion_factor']
            + merged.loc[needs_convert, 'spot_mid_rate']
        )

        # ============================
        # Optional filter by args
        # ============================
        if start:
            merged = merged[merged['timestamp'] >= pd.to_datetime(start)]
        if end:
            merged = merged[merged['timestamp'] <= pd.to_datetime(end)]

        # Save output
        out_file = self.output_dir / output_file
        merged.to_csv(out_file, index=False)

        return merged


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Rates conversion pipeline with spot merge.")
    parser.add_argument("--ccy", type=str, default="rates_ccy_data.csv", help="Currency conversion file (CSV)")
    parser.add_argument("--spot", type=str, default="rates_spot_rate_data.parq.gzip", help="Spot rate file (Parquet)")
    parser.add_argument("--price", type=str, default="rates_price_data.parq.gzip", help="Price file (Parquet)")
    parser.add_argument("--output", type=str, default="converted_prices.csv", help="Output CSV file name (under results/)")
    parser.add_argument("--start", type=str, default=None, help="Optional start datetime (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--end", type=str, default=None, help="Optional end datetime (YYYY-MM-DD HH:MM:SS)")

    args = parser.parse_args()

    processor = RatesProcessor()
    processor.process(
        ccy_file=args.ccy,
        spot_file=args.spot,
        price_file=args.price,
        output_file=args.output,
        start=args.start,
        end=args.end,
    )