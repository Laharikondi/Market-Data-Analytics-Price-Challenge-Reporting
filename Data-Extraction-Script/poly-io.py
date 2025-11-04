import requests
import pandas as pd
from datetime import datetime
import os

# =========================
# CONFIGURATION
# =========================
api_key = "295PxmgcvyjzvYafZA03NxyyxqUW2g0L"

tickers = ["AAPL", "TSLA", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "NFLX"]
vendor_id = 201
vendor_code = "Polygon"
source_feed_id = 1
exchange_code = "NASDAQ"
currency_code = "USD"

start_date = "2025-09-01"
end_date = "2025-09-30"

# =========================
# FUNCTION TO FETCH OHLC DATA
# =========================
def fetch_polygon_ohlc(ticker):
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&apiKey={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if "results" not in data:
            print(f"No OHLC data found for {ticker}")
            return pd.DataFrame()

        df = pd.DataFrame(data["results"])
        df["Security_ID"] = ticker
        df["Vendor_ID"] = vendor_id
        df["Vendor_Code"] = vendor_code
        df["Source_Feed_ID"] = source_feed_id
        df["Price_Type"] = "Close"
        df["Exchange_Code"] = exchange_code
        df["Price_Date"] = pd.to_datetime(df["t"], unit="ms")
        df["Currency_Code"] = currency_code
        df["Price"] = df["c"]

        return df[
            [
                "Security_ID",
                "Vendor_ID",
                "Vendor_Code",
                "Source_Feed_ID",
                "Price_Type",
                "Exchange_Code",
                "Price_Date",
                "Currency_Code",
                "Price",
            ]
        ]
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return pd.DataFrame()

# =========================
# FETCH DATA FOR ALL TICKERS
# =========================
all_data = []

for ticker in tickers:
    df = fetch_polygon_ohlc(ticker)
    if not df.empty:
        all_data.append(df)

# =========================
# APPEND TO EXISTING CSV
# =========================
if all_data:
    new_data = pd.concat(all_data, ignore_index=True)

    if os.path.exists("market_data.csv"):
        old_data = pd.read_csv("market_data.csv")
        combined = pd.concat([old_data, new_data], ignore_index=True)

        # OPTIONAL: remove duplicates based on (Security_ID, Price_Date)
        combined.drop_duplicates(subset=["Security_ID", "Price_Date"], keep="last", inplace=True)

        combined.to_csv("market_data.csv", index=False)
        print("[UPDATED] New data appended to market_data.csv")
    else:
        new_data.to_csv("market_data.csv", index=False)
        print("[CREATED] market_data.csv file created.")
else:
    print("[WARNING] No new data fetched.")
