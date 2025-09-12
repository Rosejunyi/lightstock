# worker.py (GitHub Actions - 最终语法和逻辑修复版)
import os
import sys
from supabase import create_client, Client
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_last_trade_date_from_db(supabase_client):
    try:
        response = supabase_client.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
        if response.data:
            return datetime.strptime(response.data[0]['date'], '%Y-%m-%d').date()
    except Exception as e:
        print(f"Warning: Could not get last trade date: {e}")
    return datetime.strptime("2024-01-01", "%Y-%m-%d").date()

def do_update_job():
    total_upserted_count = 0
    print("--- Starting data update job (GitHub Actions - Robust Backfill) ---")
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("Error: Secrets not available."); sys.exit(1)

        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        next_date_to_fill = get_last_trade_date_from_db(supabase) + timedelta(days=1)
        today = datetime.now().date()

        print(f"Last date in DB: {next_date_to_fill - timedelta(days=1)}. Starting backfill from: {next_date_to_fill}")

        if next_date_to_fill > today:
            print("Data is already up to date. Job finished.")
            return

        trade_date_df = ak.tool_trade_date_hist_df()
        trade_dates = {pd.to_datetime(d).date() for d in trade_date_df['trade_date']}

        print("Fetching valid symbols from Supabase...")
        response = supabase.table('stocks_info').select('symbol').execute()
        valid_symbols = {item['symbol'] for item in response.data}
        print(f"Found {len(valid_symbols)} valid symbols.")

        while next_date_to_fill <= today:
            if next_date_to_fill not in trade_dates:
                print(f"\nSkipping {next_date_to_fill}: Not a trading day.")
                next_date_to_fill += timedelta(days=1)
                continue

            trade_date_str = next_date_to_fill.strftime('%Y%m%d')
            print(f"\n--- Processing date: {trade_date_str} ---")
            
            try:
                stock_df = ak.stock_zh_a_hist(symbol="all", period="daily", start_date=trade_date_str, end_date=trade_date_str, adjust="")
                if stock_df is None or stock_df.empty:
                    print(f"  -> No data from AKShare for {trade_date_str}."); 
                    next_date_to_fill += timedelta(days=1)
                    continue
                
                print(f"  -> Fetched {len(stock_df)} records.")
                stock_df.rename(columns={'股票代码': 'code', '日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'volume', '成交额': 'amount'}, inplace=True)
                stock_df['volume'] = stock_df['volume'] * 100

                records_to_upsert = []
                for index, row in stock_df.iterrows():
                    code = str(row['code'])
                    market = ''
                    if code.startswith(('60', '68')): market = 'SH'
                    elif code.startswith(('00', '30')): market = 'SZ'
                    if not market: continue
                    symbol = f"{code}.{market}"
                    if symbol in valid_symbols:
                        records_to_upsert.append({
                            "symbol": symbol, "date": row['date'], "open": row['open'], "high": row['high'],
                            "low": row['low'], "close": row['close'], "volume": int(row['volume']), "amount": int(row['amount']),
                        })
                
                if records_to_upsert:
                    print(f"  -> Upserting {len(records_to_upsert)} records...")
                    supabase.table('daily_bars').upsert(records_to_upsert, on_conflict='symbol,date').execute()
                    total_upserted_count += len(records_to_upsert)
                
                next_date_to_fill += timedelta(days=1)

            except Exception as e:
                print(f"  -> An error occurred while processing {trade_date_str}: {e}")
                # --- 关键修复：补上缺失的 ") ---
                print("  -> Will retry on the next run. Stopping for now.")
                break 

    except Exception as e:
        print(f"An unhandled error occurred: {e}"); sys.exit(1)
    finally:
        print(f"\n--- Data update job FINISHED. Total new records upserted in this run: {total_upserted_count} ---")

if __name__ == '__main__':
    do_update_job()
