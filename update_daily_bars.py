# scripts/update_daily_bars.py
import os, sys, time
from supabase import create_client, Client
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_last_trade_date_from_db(supabase_client: Client):
    try:
        response = supabase_client.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
        if response.data:
            return datetime.strptime(response.data[0]['date'], '%Y-%m-%d').date()
    except Exception as e:
        print(f"Warning: Could not get last trade date: {e}")
    return datetime.strptime("2025-01-01", "%Y-%m-%d").date()

def get_valid_symbols_whitelist(supabase_client: Client):
    all_symbols = set()
    page = 0
    while True:
        response = supabase_client.table('stocks_info').select('symbol').range(page * 1000, (page + 1) * 1000 - 1).execute()
        if not response.data: break
        all_symbols.update(item['symbol'] for item in response.data)
        if len(response.data) < 1000: break
        page += 1
    return all_symbols

def main():
    print("--- Starting Job: Update Daily Bars ---")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    last_date_in_db = get_last_trade_date_from_db(supabase)
    date_to_process = last_date_in_db + timedelta(days=1)
    today = datetime.now().date()

    if date_to_process > today:
        print("Daily bars are already up to date. Job finished.")
        return
        
    print(f"Starting backfill for daily_bars from {date_to_process} up to {today}.")
    valid_symbols = get_valid_symbols_whitelist(supabase)
    print(f"Found {len(valid_symbols)} symbols to track.")
    
    while date_to_process <= today:
        trade_date_str = date_to_process.strftime('%Y%m%d')
        print(f"\n--- Processing date: {trade_date_str} ---")
        try:
            stock_df = ak.stock_zh_a_hist(symbol="all", period="daily", start_date=trade_date_str, end_date=trade_date_str, adjust="")
            if stock_df is None or stock_df.empty:
                print(f"  -> No data from AKShare for {trade_date_str} (Not a trading day).")
            else:
                print(f"  -> Fetched {len(stock_df)} records.")
                stock_df.rename(columns={'股票代码': 'code', '日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'volume', '成交额': 'amount'}, inplace=True)
                stock_df['volume'] = stock_df['volume'] * 100

                records_to_upsert = []
                for index, row in stock_df.iterrows():
                    code = str(row['code'])
                    market = 'SH' if code.startswith(('60', '68')) else 'SZ'
                    symbol = f"{code}.{market}"
                    if symbol in valid_symbols:
                        date_obj = row['date']
                        date_str_formatted = date_obj.strftime('%Y-%m-%d') if isinstance(date_obj, pd.Timestamp) else str(date_obj)
                        records_to_upsert.append({
                            "symbol": symbol, "date": date_str_formatted,
                            "open": float(row['open']), "high": float(row['high']),
                            "low": float(row['low']), "close": float(row['close']),
                            "volume": int(row['volume']), "amount": int(row['amount'])
                        })
                
                if records_to_upsert:
                    print(f"  -> Upserting {len(records_to_upsert)} valid records...")
                    supabase.table('daily_bars').upsert(records_to_upsert, on_conflict='symbol,date').execute()
        
        except Exception as e:
            print(f"  -> Error processing {date_str}: {e}. Stopping for this run.")
            break
        
        date_to_process += timedelta(days=1)
        
    print("--- Job Finished: Update Daily Bars ---")

if __name__ == '__main__':
    main()
