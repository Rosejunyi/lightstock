# worker.py (GitHub Actions - 最终的“蚂蚁搬家”稳定版)
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
    print("--- Starting data update job (Ant Moving Strategy - Final) ---")
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
        print(f"Found {len(valid_symbols)} symbols to update.")
        
        records_to_upsert = []
        for i, symbol in enumerate(valid_symbols):
            ak_code = symbol.split('.')[0]
            
            # 打印进度，\r 表示回到行首刷新，避免刷屏
            sys.stdout.write(f"\rProcessing {i+1}/{len(valid_symbols)}: {symbol}...")
            sys.stdout.flush()

            try:
                # --- 关键修复：使用我们已验证可用的单只查询函数 ---
                stock_df = ak.stock_zh_a_hist(symbol=ak_code, period="daily", start_date=trade_date_str, end_date=trade_date_str, adjust="")
                
                if stock_df is None or stock_df.empty:
                    continue # 当天停牌或无数据

                row = stock_df.iloc[0]
                records_to_upsert.append({
                    "symbol": symbol, "date": row['日期'],
                    "open": row['开盘'], "high": row['最高'], "low": row['最低'], "close": row['收盘'],
                    "volume": int(row['成交量'] * 100), "amount": int(row['成交额'])
                })
                
                # 每获取 100 条就上传一次，避免列表过大和单次请求超时
                if len(records_to_upsert) >= 100:
                    print(f"\n  -> Batch of 100 collected. Upserting...")
                    supabase.table('daily_bars').upsert(records_to_upsert).execute()
                    total_upserted_count += len(records_to_upsert)
                    records_to_upsert = [] # 清空
                    print(f"  -> Upserted successfully. Total: {total_upserted_count}")

                time.sleep(0.3) # 礼貌地延迟0.3秒，防止IP被封

            except Exception as e:
                # 打印错误但继续循环
                print(f"\n  -> Error processing {symbol}: {e}. Skipping.")
                time.sleep(1)

        # 上传最后一批不足 100 条的数据
        if records_to_upsert:
            print(f"\n  -> Upserting final batch of {len(records_to_upsert)} records...")
            supabase.table('daily_bars').upsert(records_to_upsert).execute()
            total_upserted_count += len(records_to_upsert)
        
        print("\nUpsert process completed.")

    except Exception as e:
        print(f"\nAn unhandled error occurred: {e}"); sys.exit(1)
    finally:
        print(f"\n--- Data update job FINISHED. Total new records upserted: {total_upserted_count} ---")

if __name__ == '__main__':
    do_update_job()
