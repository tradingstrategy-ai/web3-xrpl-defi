"""OHLCV data for XRPL AMM markets,
"""

from pathlib import Path
import pandas as pd


def _calc_amm_row_price(row) -> float:
    return row["base_amount"]  / row["quote_amount"]


def calculate_ohlc(price_series: pd.Series, freq: str) -> pd.DataFrame:
    """Calculate OHLC (Open, High, Low, Close) data by resampling a price series.
    
    :param price_series: 
        A pandas Series with datetime index and price values
    :param freq: 
        Frequency string for resampling (e.g., '1H', '1D', '5min', '1W')
        
    :returns: 
        DataFrame with columns: open, high, low, close
    """
    if not isinstance(price_series.index, pd.DatetimeIndex):
        raise ValueError("Price series must have a DatetimeIndex")
    
    ohlc_data = price_series.resample(freq).agg({
        'open': 'first',
        'high': 'max', 
        'low': 'min',
        'close': 'last'
    }).dropna()
    
    return ohlc_data


def calculate_quote_price(
    df: pd.DataFrame, 
    quote_token: str,
) -> pd.DataFrame:
    """Calculate quote price and swap direction for AMM.
    
    - We have CRYPTO->XRP and XRP->CRYPTO swaps mixed
    - We need to decide whether CRYPTO or XRP is the quote token
    - Calculate trade directions based on quote token
    - Calculate quoted price based on trade direction

    :param quote_token:
        Because AMM has no instrinct information in which token we want to price trades,
        we need to tell it.
    
    :returns: DataFrame with direction and quoted_price columns,
    """

    assert df["amm_asset_1"].nunique() == 1
    assert df["amm_asset_2"].nunique() == 1

    if quote_token == df["amm_asset_1"].unique()[0]:
        df = df.rename(columns={
            "amm_asset_1": "quote_asset",
            "amm_asset_2": "base_asset",
            "amm_asset_1_amount": "quote_amount",
            "amm_asset_2_amount": "base_amount",
        })
    else:
        df = df.rename(columns={
            "amm_asset_1": "base_asset",
            "amm_asset_2": "quote_asset",
            "amm_asset_1_amount": "base_amount",
            "amm_asset_2_amount": "quote_amount",
        })

    df["quoted_price"] = df.apply(_calc_amm_row_price, axis=1)
    return df





def main():
    """Manual test case.
    
    Run trades.py first.
    """
    from tabulate import tabulate
    path = Path("/tmp/amm_scan.parquet")
    df = pd.read_parquet(path)    
    price_df = calculate_quote_price(df, quote_token="XRP")
    
    pd.set_option('display.float_format', '{:.6f}'.format)
    out_df = price_df[["timestamp", "quoted_price", "base_asset", "quote_asset", "base_amount", "quote_amount", "tx_hash"]]
    
    print(tabulate(out_df.head(10), headers='keys', tablefmt='fancy_grid', showindex=False, floatfmt=".18f"))


if __name__ == "__main__":
    main()
