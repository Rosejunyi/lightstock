# scripts/update_daily_bars.py (最终的、正确的 Baostock 版)
import os
import sys
import time
from supabase import create_client, Client
import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# --- 配置 ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
# ---

def get_last_date_from_db(supabase_client: Client) -> datetime.date:
    try:
        response = supabase_client.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
        if response.data:
            return datetime.strptime(response.data[0]['date'], '%Y-%m-%d').date()
    except Exception as e:
        print(f"Warning: Could not get last date from DB: {e}")
    return datetime.strptime("2025-01-01", "%Y-%m-%d").date()

def get_valid_symbols_whitelist(supabase_client: Client) -> set:
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
    print("--- Starting Job: [1/3] Update Daily Bars (Baostock Version) ---")
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found."); sys.exit(1)
        
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 1. 登录 Baostock
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Baostock login failed: {lg.error_msg}"); sys.exit(1)
    print("Baostock login successful.")

    try:
        last_date_in_db = get_last_date_from_db(supabase)
        date_to_process = last_date_in_db + timedelta(days=1)
        today = datetime.now().date()

        if date_to_process > today:
            print("Daily bars are already up to date. Job finished."); return
            
        print(f"Starting backfill for daily_bars from {date_to_process} up to {today}.")
        valid_symbols = get_valid_symbols_whitelist(supabase)
        print(f"Found {len(valid_symbols)} symbols to track.")
        
        total_upserted_count = 0
        
        # 逐天回填
        while date_to_process <= today:
            date_str = date_to_process.strftime('%Y-%m-%d')
            print(f"\n--- Processing date: {date_str} ---")
            
            records_to_upsert = []
            
            # 逐只股票获取数据
            for i, symbol in enumerate(valid_symbols):
                sys.stdout.write(f"\r  -> Processing {i+1}/{len(valid_symbols)}: {symbol}...")
                sys.stdout.flush()

                # Baostock 需要的格式是 sh.600000
                bs_code = f"{symbol.split('.')[1].lower()}.{symbol.split('.')[0]}"
                
                rs = bs.query_history_k_data_plus(bs_code,
                    "date,code,open,high,low,close,volume,amount",
                    start_date=date_str, end_date=date_str,
                    frequency="d", adjustflag="3") # adjustflag="3" 表示不复权
                
                if rs.error_code == '0' and rs.next():
                    record = rs.get_row_data()
                    # Baostock 返回空字符串表示无数据，需要过滤
                    if record[2] != '' and record[3] != '' and record[4] != '' and record[5] != '':
                        records_to_upsert.append({
                            "symbol": symbol, "date": record[0],
                            "open": float(record[2]), "high": float(record[3]),
                            "low": float(record[4]), "close": float(record[5]),
                            "volume": int(record[6]), "amount": int(float(record[7]))
                        })
                
                # 礼貌延迟
                time.sleep(0.01)

            # 上传当天所有收集到的数据
            if records_to_upsert:
                print(f"\n  -> Upserting {len(records_to_upsert)} valid records for {date_str}...")
                supabase.table('daily_bars').upsert(records_to_upsert, on_conflict='symbol,date').execute()
                total_upserted_count += len(records_to_upsert)

            date_to_process += timedelta(days=1)
            
    finally:
        bs.logout()
        print("\nBaostock logout successful.")
        print(f"\n--- Job Finished: Update Daily Bars. Total records upserted: {total_upserted_count} ---")

if __name__ == '__main__':
    main()
