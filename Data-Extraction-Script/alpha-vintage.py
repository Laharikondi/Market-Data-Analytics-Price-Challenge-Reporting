import pandas as pd
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.foreignexchange import ForeignExchange
import os

# ======= API Key =======
api_key = 'MPEKFJF45GOQX41E'

# ======= Markets Metadata =======
markets = {
    # US Stocks
    "AAPL": {"vendor_id": 101, "vendor_code": "AV", "source_feed_id": 1, "exchange": "NASDAQ", "currency": "USD"},
    "TSLA": {"vendor_id": 101, "vendor_code": "AV", "source_feed_id": 1, "exchange": "NASDAQ", "currency": "USD"},
    "MSFT": {"vendor_id": 101, "vendor_code": "AV", "source_feed_id": 1, "exchange": "NASDAQ", "currency": "USD"},
    "AMZN": {"vendor_id": 101, "vendor_code": "AV", "source_feed_id": 1, "exchange": "NASDAQ", "currency": "USD"},
    "GOOGL": {"vendor_id": 101, "vendor_code": "AV", "source_feed_id": 1, "exchange": "NASDAQ", "currency": "USD"},
    "META": {"vendor_id": 101, "vendor_code": "AV", "source_feed_id": 1, "exchange": "NASDAQ", "currency": "USD"},
    "NVDA": {"vendor_id": 101, "vendor_code": "AV", "source_feed_id": 1, "exchange": "NASDAQ", "currency": "USD"},
    "NFLX": {"vendor_id": 101, "vendor_code": "AV", "source_feed_id": 1, "exchange": "NASDAQ", "currency": "USD"},
    # Forex
    "EURUSD": {"vendor_id": 101, "vendor_code": "AV", "source_feed_id": 2, "exchange": "FOREX", "currency": "USD"},
    "USDJPY": {"vendor_id": 101, "vendor_code": "AV", "source_feed_id": 2, "exchange": "FOREX", "currency": "JPY"},
    "USDINR": {"vendor_id": 101, "vendor_code": "AV", "source_feed_id": 2, "exchange": "FOREX", "currency": "INR"},
    # Commodities
    "XAUUSD": {"vendor_id": 101, "vendor_code": "AV", "source_feed_id": 3, "exchange": "COMEX", "currency": "USD"},
    "XAGUSD": {"vendor_id": 101, "vendor_code": "AV", "source_feed_id": 3, "exchange": "COMEX", "currency": "USD"},
}

# ======= Date Range =======
start_date = pd.to_datetime("2025-09-01")
end_date = pd.to_datetime("2025-09-30")

# ======= Initialize API Clients =======
ts = TimeSeries(key=api_key, output_format='pandas')
fx = ForeignExchange(key=api_key)

all_records = []

# ======= Function to fetch stocks/commodities =======
def fetch_stock_commodity(symbol, meta):
    try:
        data, _ = ts.get_daily(symbol=symbol, outputsize='full')
        data.reset_index(inplace=True)
        data = data.rename(columns={
            'date': 'Price_Date',
            '1. open': 'Open',
            '2. high': 'High',
            '3. low': 'Low',
            '4. close': 'Close',
            '5. volume': 'Volume'
        })
        data = data[(data['Price_Date'] >= start_date) & (data['Price_Date'] <= end_date)]
        for _, row in data.iterrows():
            for price_type in ['Open', 'Close', 'High', 'Low']:
                all_records.append({
                    'Security_ID': symbol,
                    'Vendor_ID': meta['vendor_id'],
                    'Vendor_Code': meta['vendor_code'],
                    'Source_Feed_ID': meta['source_feed_id'],
                    'Price_Type': price_type,
                    'Exchange_Code': meta['exchange'],
                    'Price_Date': row['Price_Date'],
                    'Currency_Code': meta['currency'],
                    'Price': row[price_type]
                })
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")

# ======= Function to fetch Forex =======
def fetch_forex(symbol, meta):
    try:
        from_symbol = symbol[:3]
        to_symbol = symbol[3:]
        data, _ = fx.get_currency_exchange_daily(from_symbol=from_symbol, to_symbol=to_symbol, outputsize='full')
        df = pd.DataFrame.from_dict(data, orient='index').reset_index()
        df = df.rename(columns={
            'index': 'Price_Date',
            '1. open': 'Open',
            '2. high': 'High',
            '3. low': 'Low',
            '4. close': 'Close'
        })
        df['Price_Date'] = pd.to_datetime(df['Price_Date'])
        df = df[(df['Price_Date'] >= start_date) & (df['Price_Date'] <= end_date)]
        for _, row in df.iterrows():
            for price_type in ['Open', 'Close', 'High', 'Low']:
                all_records.append({
                    'Security_ID': symbol,
                    'Vendor_ID': meta['vendor_id'],
                    'Vendor_Code': meta['vendor_code'],
                    'Source_Feed_ID': meta['source_feed_id'],
                    'Price_Type': price_type,
                    'Exchange_Code': meta['exchange'],
                    'Price_Date': row['Price_Date'],
                    'Currency_Code': meta['currency'],
                    'Price': row[price_type]
                })
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")

# ======= Loop through all markets =======
for sym, meta in markets.items():
    if meta['source_feed_id'] in [1, 3]:
        fetch_stock_commodity(sym, meta)
    else:
        fetch_forex(sym, meta)

# ======= Create DataFrame =======
df_final = pd.DataFrame(all_records)
df_final = df_final.sort_values(['Security_ID', 'Price_Date'])

# ======= Append to Existing CSV =======
output_file = "market_data.csv"

if os.path.exists(output_file):
    # Append without overwriting
    df_final.to_csv(output_file, mode='a', header=False, index=False)
    print(f"[APPENDED] Data added below existing rows in {output_file}")
else:
    # Create new file with headers
    df_final.to_csv(output_file, index=False)
    print(f"[CREATED] New file {output_file} created with data.")
