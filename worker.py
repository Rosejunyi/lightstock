# worker.py (GitHub Actions - 最终的 JSON 序列化和逻辑修复版)
import os
import sys
import time
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
    except: pass
    return datetime.strptime("2025-01-01", "%Y-%m-%d").date()

def do_update_job():
    total_upserted_count = 0
    print("--- Starting data update job (Ant Moving Strategy - Final Fix) ---")
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("Error: Secrets not available."); sys.exit(1)

        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        last_date_in_db = get_last_trade_date_from_db(supabase)
        # 我们从数据库最新日期的后一天开始，一直补到昨天
        process_date = last_date_in_db + timedelta(days=1)
        yesterday = datetime.now().date() - timedelta(days=1)

        print(f"Last date in DB: {last_date_in_db}. Starting backfill from {process_date} up to {yesterday}.")

        if process_date > yesterday:
            print("Data is up to date."); return
        
        print("Fetching valid symbols from Supabase...")
        response = supabase.table('stocks_info').select('symbol').execute()
        valid_symbols = [item['symbol'] for item in response.data]
        print(f"Found {len(valid_symbols)} symbols to update.")
        
        # 逐天处理
        while process_date <= yesterday:
            trade_date_str = process_date.strftime('%Y%m%d')
            print(f"\n--- Processing date: {trade_date_str} ---")

            records_to_upsert = []
            
            # 使用“蚂蚁搬家”策略，分小批次处理当天的股票
            batch_size = 100
            for i in range(0, len(valid_symbols), batch_size):
                batch_symbols = valid_symbols[i:i+batch_size]
                ak_codes = [s.split('.')[0] for s in batch_symbols]
                
                sys.stdout.write(f"\r  -> Fetching batch {i//batch_size + 1}/{ (len(valid_symbols) + batch_size - 1)//batch_size }...")
                sys.stdout.flush()

                try:
                    # 我们不再用 'all'，而是分批次请求
                    stock_df = ak.stock_zh_a_hist_group(symbol=",".join(ak_codes), period="daily", start_date=trade_date_str, end_date=trade_date_str, adjust="")
                    
                    if stock_df is None or stock_df.empty:
                        continue

                    for index, row in stock_df.iterrows():
                        # --- 关键修复：确保所有数据类型都是 JSON 友好的 ---
                        date_obj = row['日期']
                        date_str_formatted = date_obj.strftime('%Y-%m-%d') if isinstance(date_obj, pd.Timestamp) else str(date_obj)
                        
                        code = str(row['股票代码'])
                        market = 'SH' if code.startswith(('60', '68')) else 'SZ'
                        symbol = f"{code}.{market}"

                        records_to_upsert.append({
                            "symbol": symbol, 
                            "date": date_str_formatted,
                            "open": float(row['开盘']), "high": float(row['最高']), "low": float(row['最低']), "close": float(row['收盘']),
                            "volume": int(row['成交量'] * 100), "amount": int(row['成交额'])
                        })
                    
                    time.sleep(1) # 礼貌延迟

                except Exception as e:
                    print(f"\n  -> Error processing batch: {e}. Skipping.")
                    time.sleep(2)
            
            # 上传当天所有收集到的数据
            if records_to_upsert:
                print(f"\n  -> Upserting {len(records_to_upsert)} records for {trade_date_str}...")
                supabase.table('daily_bars').upsert(records_to_upsert).execute()
                total_upserted_count += len(records_to_upsert)

            # 日期前进一天
            process_date += timedelta(days=1)

    except Exception as e:
        print(f"\nAn unhandled error occurred: {e}"); sys.exit(1)
    finally:
        print(f"\n--- Data update job FINISHED. Total new records upserted: {total_upserted_count} ---")

if __name__ == '__main__':
    do_update_job()
