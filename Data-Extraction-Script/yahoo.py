import yfinance as yf
import pandas as pd
import os

# =========================
# SYMBOL CONFIGURATION
# =========================
all_symbols = {
    "AAPL": "Apple Inc.",
    "TSLA": "Tesla Inc.",
    "MSFT": "Microsoft Corp",
    "AMZN": "Amazon.com Inc.",
    "GOOGL": "Alphabet Inc.",
    "META": "Meta Platforms Inc.",
    "NVDA": "NVIDIA Corp",
    "NFLX": "Netflix Inc.",
    "RELIANCE.NS": "Reliance Industries (India Stock)",
    "TCS.NS": "TCS Ltd (India Stock)",
    "EURUSD=X": "EUR/USD (Forex)",
    "USDINR=X": "USD/INR (Forex)",
    "GC=F": "Gold Futures (Commodity)",
    "SI=F": "Silver Futures (Commodity)"
}

vendor_id = {"Yahoo": "VY001"}

# Exchange & Currency codes
exchange_code = {
    "AAPL": "NASDAQ", "TSLA": "NASDAQ", "MSFT": "NASDAQ", "AMZN": "NASDAQ",
    "GOOGL": "NASDAQ", "META": "NASDAQ", "NVDA": "NASDAQ", "NFLX": "NASDAQ",
    "RELIANCE.NS": "NSE", "TCS.NS": "NSE",
    "EURUSD=X": "FOREX", "USDINR=X": "FOREX",
    "GC=F": "COMEX", "SI=F": "COMEX"
}

currency_code = {
    "AAPL": "USD", "TSLA": "USD", "MSFT": "USD", "AMZN": "USD",
    "GOOGL": "USD", "META": "USD", "NVDA": "USD", "NFLX": "USD",
    "RELIANCE.NS": "INR", "TCS.NS": "INR",
    "EURUSD=X": "USD", "USDINR=X": "INR",
    "GC=F": "USD", "SI=F": "USD"
}

price_types = ['Open', 'Close', 'High', 'Low']
start_date = "2025-09-01"
end_date = "2025-09-30"

all_data = []

# =========================
# FETCH DATA
# =========================
for symbol in all_symbols.keys():
    try:
        df_raw = yf.download(symbol, start=start_date, end=end_date, progress=False, auto_adjust=False)
        if df_raw.empty:
            print(f"[WARNING] No data found for {symbol}")
            continue

        df_raw.reset_index(inplace=True)

        # Create one DataFrame per price type
        for col in price_types:
            if col not in df_raw.columns:
                print(f"[WARNING] {col} not found for {symbol}, skipping...")
                continue

            temp = pd.DataFrame({
                "Security_ID": [symbol] * len(df_raw),
                "Vendor_ID": [vendor_id["Yahoo"]] * len(df_raw),
                "Vendor_Code": ["Yahoo"] * len(df_raw),
                "Source_Feed_ID": [f"{symbol}_Yahoo"] * len(df_raw),
                "Price_Type": [col] * len(df_raw),
                "Exchange_Code": [exchange_code.get(symbol, "N/A")] * len(df_raw),
                "Price_Date": df_raw["Date"],
                "Currency_Code": [currency_code.get(symbol, "N/A")] * len(df_raw),
                "Price": df_raw[col].values.flatten()
            })

            all_data.append(temp)

    except Exception as e:
        print(f"[ERROR] {symbol}: {e}")

# =========================
# APPEND TO EXISTING FILE
# =========================
output_file = "market_data.csv"

if all_data:
    final_df = pd.concat(all_data, ignore_index=True)

    # Ensure consistent column ordering
    final_df = final_df[[
        "Security_ID", "Vendor_ID", "Vendor_Code", "Source_Feed_ID",
        "Price_Type", "Exchange_Code", "Price_Date",
        "Currency_Code", "Price"
    ]]

    # If file exists, append without overwriting
    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)
        
        # Align columns safely
        for col in final_df.columns:
            if col not in existing_df.columns:
                existing_df[col] = None
        for col in existing_df.columns:
            if col not in final_df.columns:
                final_df[col] = None

        combined_df = pd.concat([existing_df, final_df], ignore_index=True)
        combined_df.to_csv(output_file, index=False)
        print(f"[UPDATED] Appended new Yahoo data to {output_file}")
        print(f"→ Total rows after update: {len(combined_df)}")
    else:
        final_df.to_csv(output_file, index=False)
        print(f"[CREATED] New {output_file} file created.")
else:
    print("[WARNING] No data fetched.")
