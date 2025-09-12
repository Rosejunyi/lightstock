# worker.py (GitHub Actions - 最终完美健壮回填版)
import os
import sys
from supabase import create_client, Client
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_last_trade_date_from_db(supabase_client):
    """ 从 daily_bars 获取最新的一个交易日 """
    try:
        response = supabase_client.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
        if response.data:
            return datetime.strptime(response.data[0]['date'], '%Y-%m-%d').date()
    except Exception as e:
        print(f"Warning: Could not get last trade date: {e}")
    # 安全的创世日期，如果数据库是空的，就从这一天开始
    return datetime.strptime("2025-01-01", "%Y-%m-%d").date()

def do_update_job():
    total_upserted_count = 0
    print("--- Starting data update job (GitHub Actions - Robust Backfill) ---")
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("Error: Secrets not available."); sys.exit(1)

        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        # 1. 智能地确定需要开始回填的日期
        last_date_in_db = get_last_trade_date_from_db(supabase)
        next_date_to_fill = last_date_in_db + timedelta(days=1)
        today = datetime.now().date()

        print(f"Last date in DB is {last_date_in_db}. Starting backfill from {next_date_to_fill}.")

        if next_date_to_fill > today:
            print("Data is already up to date. Job finished.")
            return

        # 2. 放弃交易日历，直接循环每一天
        print("Fetching valid symbols from Supabase...")
        response = supabase.table('stocks_info').select('symbol').execute()
        valid_symbols = {item['symbol'] for item in response.data}
        print(f"Found {len(valid_symbols)} valid symbols to track.")

        current_date_to_process = next_date_to_fill
        while current_date_to_process <= today:
            trade_date_str = current_date_to_process.strftime('%Y%m%d')
            print(f"\n--- Processing date: {trade_date_str} ---")
            
            try:
                stock_df = ak.stock_zh_a_hist(symbol="all", period="daily", start_date=trade_date_str, end_date=trade_date_str, adjust="")
                
                if stock_df is None or stock_df.empty:
                    print(f"  -> No data from AKShare for {trade_date_str}. Skipping (Not a trading day).")
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
                        print(f"  -> Upserting {len(records_to_upsert)} records...")
                        supabase.table('daily_bars').upsert(records_to_upsert, on_conflict='symbol,date').execute()
                        total_upserted_count += len(records_to_upsert)
            
            except Exception as e:
                print(f"  -> An error occurred while processing {trade_date_str}: {e}")
                print("  -> Will retry this date on the next run. Stopping for now.")
                break
            
            current_date_to_process += timedelta(days=1)

    except Exception as e:
        print(f"An unhandled error occurred in background job: {e}"); sys.exit(1)
    finally:
        print(f"\n--- Data update job FINISHED. Total new records upserted in this run: {total_upserted_count} ---")

if __name__ == '__main__':
    do_update_job()
