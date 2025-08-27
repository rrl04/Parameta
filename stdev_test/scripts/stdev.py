# stdev.py
import pandas as pd
from pathlib import Path
import argparse


class StdDevProcessor:
    def __init__(self, data_dir: str = None, output_dir: str = None):
        '''
        Initialize processor with data and output directories.
        Defaults to <repo_root>/data and <repo_root>/results.
        '''
        repo_root = Path(__file__).resolve().parents[1]  # stdev_test/
        self.data_dir = Path(data_dir) if data_dir else repo_root / 'data'
        self.output_dir = Path(output_dir) if output_dir else repo_root / 'results'
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _load(self, filename: str) -> pd.DataFrame:
        '''
        Load parquet file into a pandas DataFrame.
        Ensures timestamp is converted to datetime.
        '''
        path = self.data_dir / filename
        if not path.exists():
            raise FileNotFoundError(f'File not found: {path}')

        df = pd.read_parquet(path, engine='pyarrow')

        # ensure timestamp exists and is datetime
        if 'snap_time' in df.columns:
            df['snap_time'] = pd.to_datetime(df['snap_time'], errors='coerce')

        return df

    def _compute_group_stdev(self, grp: pd.DataFrame, add_gap_flag: bool = False) -> pd.DataFrame:
        """
        Compute rolling stdevs for one security_id.
        Rule: Only compute 20-row stdev if all 20 rows are hourly contiguous.
        Optimized to run < 1s even for large data.
        """
        grp = grp.sort_values("snap_time").reset_index(drop=True)

        # boolean mask: did this row follow exactly 1h after prev?
        one_hour_gap = grp['snap_time'].diff().eq(pd.Timedelta('1h')).astype(int)

        # rolling sum of last 19 gaps; need 19/19 = contiguous
        contiguous_20 = one_hour_gap.rolling(19, min_periods=19).sum().eq(19)

        results = []
        for col in ['bid', 'mid', 'ask']:
            roll_std = grp[col].rolling(20, min_periods=20).std()
            # mask out windows that aren't fully contiguous
            roll_std = roll_std.where(contiguous_20)
            results.append(roll_std.rename(f"{col}_stdev"))

        out = pd.concat([grp, *results], axis=1)

        if add_gap_flag:
            # True if rolling window was blocked by a gap
            out['gap_blocked'] = (~contiguous_20) & out[['bid_stdev','mid_stdev','ask_stdev']].notna().any(axis=1)

        return out

    def process(
        self,
        input_file: str = "stdev_price_data.parq.gzip",
        output_file: str = "rolling_stdev.csv",
        start: str = "2021-11-20 00:00:00",
        end: str = "2021-11-23 09:00:00",
        add_gap_flag: bool = False
    ):
        '''
        Main pipeline:
          - load price data
          - compute rolling stdevs
          - save to CSV
        '''
        df = self._load(input_file)

        # Ensure proper sorting
        df = df.sort_values(['security_id', 'snap_time'])

        # Apply per security_id
        all_results = []
        for sec_id, grp in df.groupby('security_id', group_keys=False):
            grp_result = self._compute_group_stdev(grp, add_gap_flag=add_gap_flag)
            all_results.append(grp_result)

        result = pd.concat(all_results, ignore_index=True)

        # Filter by requested window
        result = result[(result['snap_time'] >= pd.to_datetime(start)) &
                        (result['snap_time'] <= pd.to_datetime(end))]

        # Save output
        out_file = self.output_dir / output_file
        result.to_csv(out_file, index=False)

        return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Compute rolling stdevs with gap handling.")
    parser.add_argument("--input", type=str, default="stdev_price_data.parq.gzip", help="Input parquet file name (under data/)")
    parser.add_argument("--output", type=str, default="rolling_stdev.csv", help="Output CSV file name (under results/)")
    parser.add_argument("--start", type=str, default="2021-11-20 00:00:00", help="Start datetime (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--end", type=str, default="2021-11-23 09:00:00", help="End datetime (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--add-gap-flag", action="store_true", help="Include diagnostic flag for gap-blocked windows")

    args = parser.parse_args()

    processor = StdDevProcessor()
    processor.process(
        input_file=args.input_file,
        output_file=args.output_file,
        start=args.start,
        end=args.end,
        add_gap_flag=args.add_gap_flag
    )