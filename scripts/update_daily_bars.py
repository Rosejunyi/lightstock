# scripts/update_daily_bars.py (最终的 Baostock 版)
import os, sys, time
from supabase import create_client, Client
import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_last_date_from_db(supabase_client: Client):
    # ... (这个函数和之前一样)

def get_valid_symbols_whitelist(supabase_client: Client):
    # ... (这个函数和之前一样)

def main():
    print("--- Starting Job: [1/3] Update Daily Bars (Baostock Version) ---")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 1. 登录 Baostock
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Baostock login failed: {lg.error_msg}"); sys.exit(1)
    print("Baostock login successful.")

    try:
        last_date = get_last_date_from_db(supabase)
        date_to_process = last_date + timedelta(days=1)
        today = datetime.now().date()
        
        valid_symbols = get_valid_symbols_whitelist(supabase)
        print(f"Found {len(valid_symbols)} symbols to update.")
        
        records_to_upsert = []
        while date_to_process <= today:
            date_str = date_to_process.strftime('%Y-%m-%d')
            print(f"\n--- Processing date: {date_str} ---")
            
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
                    records_to_upsert.append({
                        "symbol": symbol, "date": record[0],
                        "open": float(record[2]), "high": float(record[3]),
                        "low": float(record[4]), "close": float(record[5]),
                        "volume": int(record[6]), "amount": int(float(record[7]))
                    })
                
                # 每 100 条上传一次
                if len(records_to_upsert) >= 100:
                    supabase.table('daily_bars').upsert(records_to_upsert).execute()
                    records_to_upsert = []
            
            date_to_process += timedelta(days=1)
        
        # 上传最后一批
        if records_to_upsert:
            supabase.table('daily_bars').upsert(records_to_upsert).execute()

    finally:
        bs.logout()
        print("\nBaostock logout successful.")
        print("--- Job Finished: Update Daily Bars ---")

if __name__ == '__main__':
    main()
