# worker.py (GitHub Actions - 最终完美、最简健壮版)
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
    # 安全的创世日期
    return datetime.strptime("2025-01-01", "%Y-%m-%d").date()

def do_update_job():
    total_upserted_count = 0
    print("--- Starting data update job (Final Robust Version) ---")
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("Error: Secrets not available."); sys.exit(1)

        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        last_date_in_db = get_last_trade_date_from_db(supabase)
        current_date_to_process = last_date_in_db + timedelta(days=1)
        # 我们检查到“昨天”的数据，以确保日线数据已经生成
        process_until_date = datetime.now().date() - timedelta(days=1)

        print(f"Last date in DB is {last_date_in_db}. Starting backfill from {current_date_to_process} up to {process_until_date}.")

        if current_date_to_process > process_until_date:
            print("Data is already up to date. Job finished."); return

        print("Fetching valid symbols from Supabase...")
        response = supabase.table('stocks_info').select('symbol').execute()
        valid_symbols = {item['symbol'] for item in response.data}
        print(f"Found {len(valid_symbols)} valid symbols to track.")

        # --- 关键修复：不再查询交易日历，直接循环每一天 ---
        while current_date_to_process <= process_until_date:
            trade_date_str = current_date_to_process.strftime('%Y%m%d')
            print(f"\n--- Processing date: {trade_date_str} ---")
            
            try:
                # 直接尝试获取当天数据
                stock_df = ak.stock_zh_a_hist(symbol="all", period="daily", start_date=trade_date_str, end_date=trade_date_str, adjust="")
                
                # 如果返回空，说明当天不是交易日，直接跳到下一天
                if stock_df is None or stock_df.empty:
                    print(f"  -> No data from AKShare for {trade_date_str}. Skipping (Not a trading day).")
                else:
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
            
            except Exception as e:
                print(f"  -> An error occurred while processing {trade_date_str}: {e}")
                print("  -> Will retry this date on the next run. Stopping for now.")
                break
            
            # 无论成功或失败，都继续检查下一天
            current_date_to_process += timedelta(days=1)

    except Exception as e:
        print(f"An unhandled error occurred in background job: {e}"); sys.exit(1)
    finally:
        print(f"\n--- Data update job FINISHED. Total new records upserted in this run: {total_upserted_count} ---")

if __name__ == '__main__':
    do_update_job()
