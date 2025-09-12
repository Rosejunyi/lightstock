# worker.py (GitHub Actions - 最终的“智能分批”版)
import os
import sys
import time
from supabase import create_client, Client
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def do_update_job():
    total_upserted_count = 0
    print("--- Starting data update job (Smart Batching Strategy) ---")
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("Error: Secrets not available."); sys.exit(1)

        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        # --- 关键：我们先只测试补全 9月9日 这一天 ---
        target_date = datetime(2025, 9, 9).date()
        trade_date_str = target_date.strftime('%Y%m%d')
        print(f"Targeting a single day for backfill: {trade_date_str}")

        print("Fetching valid symbols from Supabase...")
        response = supabase.table('stocks_info').select('symbol').execute()
        valid_symbols = [item['symbol'] for item in response.data]
        # 将 600000.SH -> 600000
        ak_codes = [s.split('.')[0] for s in valid_symbols]
        print(f"Found {len(valid_symbols)} symbols to update.")
        
        # --- 核心：智能分批 ---
        batch_size = 100 # 每次请求 100 只股票
        total_batches = (len(ak_codes) + batch_size - 1) // batch_size
        all_records_to_upsert = []

        for i in range(0, len(ak_codes), batch_size):
            batch_codes = ak_codes[i:i+batch_size]
            current_batch_num = i//batch_size + 1
            print(f"\n--- Processing batch {current_batch_num}/{total_batches} for date: {trade_date_str} ---")
            
            try:
                # 使用能批量获取少量股票的接口
                batch_str = ",".join(batch_codes)
                stock_df = ak.stock_zh_a_hist_group(symbol=batch_str, period="daily", start_date=trade_date_str, end_date=trade_date_str, adjust="")
                
                if stock_df is None or stock_df.empty:
                    print(f"  -> No data returned for this batch. Skipping.")
                    continue
                
                print(f"  -> Fetched {len(stock_df)} records for this batch.")
                
                # ak.stock_zh_a_hist_group 返回的数据格式与单个查询略有不同
                stock_df.rename(columns={'股票代码': 'code', '日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'volume', '成交额': 'amount'}, inplace=True)
                stock_df['volume'] = stock_df['volume'] * 100

                for index, row in stock_df.iterrows():
                    code = str(row['code'])
                    market = ''
                    if code.startswith(('60', '68')): market = 'SH'
                    elif code.startswith(('00', '30')): market = 'SZ'
                    if not market: continue
                    symbol = f"{code}.{market}"
                    
                    all_records_to_upsert.append({
                        "symbol": symbol, "date": row['date'], "open": row['open'], "high": row['high'],
                        "low": row['low'], "close": row['close'], "volume": int(row['volume']), "amount": int(row['amount']),
                    })

                time.sleep(2) # 每批次请求后休息2秒

            except Exception as e:
                print(f"  -> An error occurred while processing batch {current_batch_num}: {e}")
                time.sleep(5)

        print(f"\nTotal valid records to upsert: {len(all_records_to_upsert)}")
        
        if all_records_to_upsert:
            print("Upserting all collected data to daily_bars table...")
            upsert_batch_size = 500
            for i in range(0, len(all_records_to_upsert), upsert_batch_size):
                batch = all_records_to_upsert[i:i+upsert_batch_size]
                supabase.table('daily_bars').upsert(batch, on_conflict='symbol,date').execute()
            total_upserted_count = len(all_records_to_upsert)
            print("Upsert completed successfully.")

    except Exception as e:
        print(f"An unhandled error occurred in background job: {e}"); sys.exit(1)
    finally:
        print(f"\n--- Data update job FINISHED. Total new records upserted in this run: {total_upserted_count if 'total_upserted_count' in locals() else 0} ---")

if __name__ == '__main__':
    do_update_job()
