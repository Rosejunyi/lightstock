# worker.py (GitHub Actions - 最终语法和类型修复版)
import os
import sys
from supabase import create_client, Client
import akshare as ak
import pandas as pd
from datetime import datetime

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def do_update_job():
    print("--- Starting data update job (GitHub Actions) ---")
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("Error: SUPABASE_URL and SUPABASE_KEY secrets are not available.")
            sys.exit(1)

        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        print("Fetching all stock data from AKShare...")
        stock_df = ak.stock_zh_a_spot_em()
        if stock_df is None or stock_df.empty:
            print("Failed to fetch data from AKShare."); return
        print(f"Fetched {len(stock_df)} records from AKShare.")
        
        required_fields = ['代码', '今开', '最高', '最低', '最新价', '成交量', '成交额']
        stock_df.dropna(subset=required_fields, inplace=True)
        print(f"Cleaned data, {len(stock_df)} valid records remaining.")

        records_to_upsert = []
        today = datetime.now().strftime('%Y-%m-%d')

        for index, row in stock_df.iterrows():
            code = str(row['代码'])
            open_price = float(row['今开'])
            
            if open_price == 0: continue

            market = ''
            if code.startswith(('60', '68')): market = 'SH'
            elif code.startswith(('00', '30')): market = 'SZ'
            if not market: continue
            
            volume_val = int(row['成交量'])
            amount_val = int(float(row['成交额']))
            
            records_to_upsert.append({
                "symbol": f"{code}.{market}", "date": today,
                "open": open_price, "high": float(row['最高']),
                "low": float(row['最低']), "close": float(row['最新价']),
                "volume": volume_val,
                "amount": amount_val,
            })
        
        print(f"Total valid records to upsert: {len(records_to_upsert)}")
        
        if records_to_upsert:
            # --- 关键修复：补上缺失的 ") ---
            print("Upserting data to daily_bars table...")
            batch_size = 500
            for i in range(0, len(records_to_upsert), batch_size):
                batch = records_to_upsert[i:i+batch_size]
                supabase.table('daily_bars').upsert(batch, on_conflict='symbol,date').execute()
            print("Upsert completed successfully.")
    except Exception as e:
        print(f"An error occurred in background job: {e}")
        sys.exit(1)
    finally:
        print("--- Data update job FINISHED ---")

if __name__ == '__main__':
    do_update_job()
