"""OHLCV data for XRPL AMM markets,
"""
import pandas as pd

import plotly.express as px


def visualise_ohlc(df: pd.DataFrame, title: str) -> None:
    """Visualise OHLCV data using matplotlib.

    :param df: DataFrame with columns: open, high, low, close
    :param title: Title of the chart
    """
    fig = px.candlestick(
        df, x=df.index, 
        open=df.open, 
        high=df.high, 
        low=df.low, 
        close=df.close, 
        title=title
    )

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Price",
        xaxis_rangeslider_visible=False,
        title=title,
    )
    return fig