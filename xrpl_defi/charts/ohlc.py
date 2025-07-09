"""OHLCV data for XRPL AMM markets,
"""
import pandas as pd

import plotly.express as px
import plotly.graph_objects as go


def visualise_ohlc(df: pd.DataFrame, title: str, base: str=None, quote: str=None, width=1200, height=500) -> None:
    """Visualise OHLCV data using matplotlib.

    :param df: DataFrame with columns: open, high, low, close
    :param title: Title of the chart
    :return: Plotly Figure instance
    """

    assert isinstance(df.index, pd.DatetimeIndex), f"Expected DateTimeIndex, got {type(df.index)}"

    # Create candlestick chart
    fig = go.Figure(data=[
        go.Candlestick(
            x=df.index,
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close']
        )
    ])

    y_axis_title = f"Price {base}/{quote}" if base and quote else "Price"

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title=y_axis_title,
        xaxis_rangeslider_visible=False,
        title=title,
        width=width,
        height=height,
    )
    return fig