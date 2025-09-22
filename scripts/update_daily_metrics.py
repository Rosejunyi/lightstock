# scripts/update_daily_metrics.py (最终的、Baostock 参数修复版)
import os, sys
from supabase import create_client, Client
import baostock as bs
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# ... (配置加载和 get_valid_symbols_whitelist 函数保持不变) ...

def main(supabase_url: str, supabase_key: str):
    print("--- Starting Job: [2/3] Update Daily Metrics (Baostock Version) ---")
    supabase: Client = create_client(supabase_url, supabase_key)
    valid_symbols_whitelist = get_valid_symbols_whitelist(supabase)
    
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Baostock login failed: {lg.error_msg}"); sys.exit(1)
    print("Baostock login successful.")

    try:
        metrics_date = datetime.now().date()
        date_str_for_db = metrics_date.strftime('%Y-%m-%d')
        # Baostock 的 date 参数，在某些函数中是可选的，我们先尝试不带
            
        print(f"Fetching latest daily metrics for {len(valid_symbols_whitelist)} stocks...")
        
        # --- 核心修复：调用 query_stock_basic 时，不传入 date 参数 ---
        # 这个函数默认就会返回最新的基本面数据
        rs = bs.query_stock_basic()
        # -----------------------------------------------------------
        
        if rs.error_code != '0':
            print(f"Failed to fetch stock basics from Baostock: {rs.error_msg}")
            return

        stock_basics_df = rs.get_data()
        print(f"Fetched {len(stock_basics_df)} total metric records from Baostock.")

        # --- 核心修复：Baostock 返回的 code 是纯数字，我们需要自己拼接 ---
        records_to_upsert = []
        for index, row in stock_basics_df.iterrows():
            code = str(row['code'])
            # Baostock 的 code 格式是 sh.600001, 我们需要转换
            if '.' in code:
                parts = code.split('.')
                symbol = f"{parts[1]}.{parts[0].upper()}" # sh.600001 -> 600001.SH
            else: continue
            
            if symbol not in valid_symbols_whitelist:
                continue

            try:
                # ... (后续的数据清洗和准备逻辑，和之前一样) ...
                record = { ... } # 省略
                records_to_upsert.append(record)
            except: continue
        
        if records_to_upsert:
            print(f"Upserting {len(records_to_upsert)} valid metric records to daily_metrics...")
            # ... (分批上传逻辑) ...
            print("daily_metrics table updated successfully!")
            
    finally:
        bs.logout()
        print("\nBaostock logout successful.")
        print("--- Job Finished: Update Daily Metrics ---")

if __name__ == '__main__':
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found."); sys.exit(1)
    main(SUPABASE_URL, SUPABASE_KEY)
