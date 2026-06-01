import requests
import pandas as pd
from datetime import datetime, timedelta

def fetch_ohlcv(symbol="BTCUSDT", interval="1d", limit=100):
    """Забирает OHLCV данные с Binance REST API"""
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    data = response.json()
    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "taker_buy_base", 
        "taker_buy_quote", "ignore"
    ])
    
    # Конвертация типов
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
        
    df["symbol"] = symbol
    return df[["open_time", "symbol", "open", "high", "low", "close", "volume"]]