import requests
import pandas as pd
import os

# ======= API Key and Endpoint =======
API_KEY = "ih0dviWTgveqD24bFzKgCIDd0x6Z3DSxRYGplZFk"
API_URL = "https://api.stockdata.org/v1/data/eod"

# ======= Stock Symbols =======
symbols = ["AAPL", "TSLA", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "NFLX"]

# ======= Date Range =======
start_date = "2025-09-01"
end_date = "2025-09-30"

# ======= Metadata =======
VENDOR_ID = 103
VENDOR_CODE = "StockData"
SOURCE_FEED_ID = 1
CURRENCY_CODE = "USD"

all_records = []

# ======= Fetch Data =======
for symbol in symbols:
    params = {
        "symbols": symbol,
        "api_token": API_KEY,
        "date_from": start_date,
        "date_to": end_date
    }

    try:
        response = requests.get(API_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        if "data" in data:
            for row in data["data"]:
                for price_type, key in zip(["Open", "Close", "High", "Low"], ["open", "close", "high", "low"]):
                    price_value = row.get(key)
                    if price_value is not None:
                        all_records.append({
                            "Security_ID": symbol,
                            "Vendor_ID": VENDOR_ID,
                            "Vendor_Code": VENDOR_CODE,
                            "Source_Feed_ID": SOURCE_FEED_ID,
                            "Price_Type": price_type,
                            "Exchange_Code": row.get("mic_code", "IEXG"),
                            "Price_Date": pd.to_datetime(row["date"]),
                            "Currency_Code": row.get("currency", CURRENCY_CODE),
                            "Price": price_value
                        })
        else:
            print(f"[INFO] No data returned for {symbol}.")

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Error fetching {symbol}: {e}")

# ======= Append or Create CSV =======
if all_records:
    new_data = pd.DataFrame(all_records)
    new_data = new_data.sort_values(["Security_ID", "Price_Date"])

    file_path = "market_data.csv"

    if os.path.exists(file_path):
        # Read existing data
        existing_data = pd.read_csv(file_path)

        # Combine old and new
        combined = pd.concat([existing_data, new_data], ignore_index=True)

        # Remove duplicates (optional)
        combined.drop_duplicates(
            subset=["Security_ID", "Price_Date", "Price_Type"],
            keep="last",
            inplace=True
        )

        combined.to_csv(file_path, index=False)
        print(f"[UPDATED] Appended new data to {file_path}")
        print(f"→ Total rows after update: {len(combined)}")

    else:
        # Create new file
        new_data.to_csv(file_path, index=False)
        print(f"[CREATED] New file saved as {file_path}")
        print(f"→ Total rows: {len(new_data)}")
else:
    print("[WARNING] No data fetched. Check API key, symbols, or date range.")
