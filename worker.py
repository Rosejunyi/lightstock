# worker.py (GitHub Actions - 最终的“王者归来”版)
import os
import sys
from supabase import create_client, Client
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def do_update_job():
    total_upserted_count = 0
    print("--- Starting data update job (The Final, Correct Version) ---")
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("Error: Secrets not available."); sys.exit(1)

        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        # --- 关键：严格按照你的需求，定义回填范围 ---
        dates_to_process = [
            datetime(2025, 9, 9).date(),
            datetime(2025, 9, 10).date(),
            datetime(2025, 9, 11).date(),
            datetime(2025, 9, 12).date(),
        ]
        print(f"Targeting specific dates for backfill: {[d.strftime('%Y-%m-%d') for d in dates_to_process]}")
        
        print("Fetching valid symbols from Supabase for filtering...")
        response = supabase.table('stocks_info').select('symbol').execute()
        valid_symbols = {item['symbol'] for item in response.data}
        print(f"Found {len(valid_symbols)} valid symbols.")

        for trade_date in dates_to_process:
            trade_date_str = trade_date.strftime('%Y%m%d')
            print(f"\n--- Processing date: {trade_date_str} ---")

            try:
                stock_df = ak.stock_zh_a_hist(symbol="all", period="daily", start_date=trade_date_str, end_date=trade_date_str, adjust="")
                if stock_df is None or stock_df.empty:
                    print(f"  -> No data from AKShare for {trade_date_str}. Skipping."); continue
                
                print(f"  -> Fetched {len(stock_df)} raw records.")
                
                required_fields = ['股票代码', '日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额']
                stock_df.dropna(subset=required_fields, inplace=True)
                
                stock_df.rename(columns={'股票代码': 'code', '日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'volume', '成交额': 'amount'}, inplace=True)
                stock_df['volume'] = stock_df['volume'] * 100

                records_to_upsert = []
                for index, row in stock_df.iterrows():
                    code = str(row['code'])
                    market = 'SH' if code.startswith(('60', '68')) else 'SZ'
                    symbol = f"{code}.{market}"
                    
                    if symbol in valid_symbols:
                        # --- 核心修复：确保所有数据类型都是 JSON 友好的 ---
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
                    total_upserted_count += len(records_to_upsert)
            
            except Exception as e:
                print(f"  -> An error occurred while processing {trade_date_str}: {e}")

    except Exception as e:
        print(f"An unhandled error occurred in background job: {e}"); sys.exit(1)
    finally:
        print(f"\n--- Data update job FINISHED. Total new records upserted: {total_upserted_count} ---")

if __name__ == '__main__':
    do_update_job()
