# worker.py (GitHub Actions - 最终的“定点回填”版)
import os
import sys
from supabase import create_client, Client
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# --- 关键配置：我们在这里手动指定回填的范围 ---
# 我们强制从 9月9日 开始
BACKFILL_START_DATE = datetime(2025, 9, 9).date()
# 回填到今天为止
BACKFILL_END_DATE = datetime.now().date()
# ------------------------------------------------

def do_update_job():
    total_upserted_count = 0
    print("--- Starting data update job (GitHub Actions - Targeted Backfill) ---")
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("Error: Secrets not available."); sys.exit(1)

        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        # 1. 从 stocks_info 获取“蓝图”
        print("Fetching valid symbols from Supabase...")
        response = supabase.table('stocks_info').select('symbol').execute()
        valid_symbols = {item['symbol'] for item in response.data}
        print(f"Found {len(valid_symbols)} valid symbols to track.")

        # 2. 生成我们需要处理的日期列表
        dates_to_process = []
        current_date = BACKFILL_START_DATE
        while current_date <= BACKFILL_END_DATE:
            dates_to_process.append(current_date)
            current_date += timedelta(days=1)

        if not dates_to_process:
            print("No dates to process. Job finished.")
            return

        print(f"Targeting {len(dates_to_process)} days for backfill: from {BACKFILL_START_DATE} to {BACKFILL_END_DATE}")

        # 3. 逐个交易日进行数据获取和上传
        for trade_date in dates_to_process:
            trade_date_str = trade_date.strftime('%Y%m%d')
            print(f"\n--- Processing date: {trade_date_str} ---")
            
            try:
                # 直接尝试获取当天数据
                stock_df = ak.stock_zh_a_hist(symbol="all", period="daily", start_date=trade_date_str, end_date=trade_date_str, adjust="")
                
                if stock_df is None or stock_df.empty:
                    print(f"  -> No data from AKShare for {trade_date_str}. Skipping (Not a trading day).")
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
                    print(f"  -> Upserting {len(records_to_upsert)} valid records for {trade_date_str}...")
                    supabase.table('daily_bars').upsert(records_to_upsert, on_conflict='symbol,date').execute()
                    total_upserted_count += len(records_to_upsert)
            
            except Exception as e:
                print(f"  -> An error occurred while processing {trade_date_str}: {e}")
                # 即使某一天失败了，我们也继续尝试下一天
                continue 

    except Exception as e:
        print(f"An unhandled error occurred in background job: {e}"); sys.exit(1)
    finally:
        print(f"\n--- Data update job FINISHED. Total new records upserted in this run: {total_upserted_count} ---")

if __name__ == '__main__':
    do_update_job()
