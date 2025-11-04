import requests
import pandas as pd
import os

# =============================
# API Key & Symbols
# =============================
api_key = "0f422370df4341f4825505ca320eefba"

symbols = [
    "AAPL", "TSLA", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "NFLX",
    "RELIANCE.NS", "TCS.NS", "GC=F", "SI=F", "EURUSD=X", "USDINR=X"
]

# Vendor Info
vendor_id = "TD001"
vendor_code = "TwelveData"

# Exchange & Currency mapping
exchange_code = {
    "AAPL": "NASDAQ", "TSLA": "NASDAQ", "MSFT": "NASDAQ", "AMZN": "NASDAQ",
    "GOOGL": "NASDAQ", "META": "NASDAQ", "NVDA": "NASDAQ", "NFLX": "NASDAQ",
    "RELIANCE.NS": "NSE", "TCS.NS": "NSE",
    "GC=F": "COMEX", "SI=F": "COMEX",
    "EURUSD=X": "FOREX", "USDINR=X": "FOREX"
}

currency_code = {
    "AAPL": "USD", "TSLA": "USD", "MSFT": "USD", "AMZN": "USD",
    "GOOGL": "USD", "META": "USD", "NVDA": "USD", "NFLX": "USD",
    "RELIANCE.NS": "INR", "TCS.NS": "INR",
    "GC=F": "USD", "SI=F": "USD",
    "EURUSD=X": "USD", "USDINR=X": "INR"
}

# Date range
start_date = "2025-09-01"
end_date = "2025-09-30"

all_data = []

# =============================
# Fetch data from Twelve Data
# =============================
for symbol in symbols:
    url = (
        f"https://api.twelvedata.com/time_series?"
        f"symbol={symbol}&interval=1day&start_date={start_date}&end_date={end_date}&apikey={api_key}"
    )

    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()

        if "values" in data:
            df = pd.DataFrame(data["values"])
            df["Security_ID"] = symbol
            df["Vendor_ID"] = vendor_id
            df["Vendor_Code"] = vendor_code
            df["Source_Feed_ID"] = f"{symbol}_{vendor_code}"
            df["Exchange_Code"] = exchange_code.get(symbol, "NA")
            df["Currency_Code"] = currency_code.get(symbol, "NA")

            # Expand into multiple rows per price type
            temp_list = []
            for _, row in df.iterrows():
                for price_type in ["open", "high", "low", "close"]:
                    temp_list.append({
                        "Security_ID": row["Security_ID"],
                        "Vendor_ID": row["Vendor_ID"],
                        "Vendor_Code": row["Vendor_Code"],
                        "Source_Feed_ID": row["Source_Feed_ID"],
                        "Price_Type": price_type.capitalize(),
                        "Exchange_Code": row["Exchange_Code"],
                        "Price_Date": row["datetime"],
                        "Currency_Code": row["Currency_Code"],
                        "Price": row[price_type]
                    })
            all_data.extend(temp_list)
        else:
            print(f"[INFO] No data for {symbol}: {data.get('message', 'Unknown error')}")

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to fetch {symbol}: {e}")

# =============================
# Save or Append to market_data.csv
# =============================
if all_data:
    new_df = pd.DataFrame(all_data)
    new_df["Price_Date"] = pd.to_datetime(new_df["Price_Date"])
    new_df = new_df.sort_values(["Security_ID", "Price_Date", "Price_Type"])

    file_path = "market_data.csv"

    if os.path.exists(file_path):
        # Read existing file
        existing_df = pd.read_csv(file_path)

        # Combine both
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)

        # Remove duplicates (keep the latest record)
        combined_df.drop_duplicates(
            subset=["Security_ID", "Price_Date", "Price_Type"],
            keep="last",
            inplace=True
        )

        combined_df.to_csv(file_path, index=False)
        print(f"[UPDATED] Appended new TwelveData data to {file_path}")
        print(f"→ Total rows after update: {len(combined_df)}")
    else:
        # Save as new file if it doesn't exist
        new_df.to_csv(file_path, index=False)
        print(f"[CREATED] New file saved as {file_path}")
        print(f"→ Total rows: {len(new_df)}")
else:
    print("[WARNING] No data fetched. Check API key, symbols, or date range.")
