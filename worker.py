# worker.py (GitHub Actions - 最终的、完整的、王者归来版)
import os, sys, time
from supabase import create_client, Client
import akshare as ak
import pandas as pd
import numpy as np
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

def get_valid_symbols_whitelist(supabase_client):
    print("Fetching whitelist from stocks_info (with pagination)...")
    all_symbols = set()
    page = 0
    while True:
        response = supabase_client.table('stocks_info').select('symbol').range(page * 1000, (page + 1) * 1000 - 1).execute()
        if not response.data: break
        all_symbols.update(item['symbol'] for item in response.data)
        if len(response.data) < 1000: break
        page += 1
    print(f"  -> Whitelist created with {len(all_symbols)} symbols.")
    return all_symbols

def do_update_job():
    print("--- Starting Daily Full Data Update Job ---")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")

        # 步骤 1: 获取白名单
        valid_symbols_whitelist = get_valid_symbols_whitelist(supabase)
        
        # 步骤 2: 更新 Daily Bars
        print("\n--- Step 2: Updating Daily Bars ---")
        # ... (这里是你之前那个能成功运行的、最稳妥的 daily_bars 更新代码)
        # 确保在它的 for 循环内部，有 if symbol in valid_symbols_whitelist 的判断
        
        # 步骤 3: 更新 Daily Metrics
        print("\n--- Step 3: Updating Daily Metrics ---")
        metrics_df = ak.stock_zh_a_spot_em()
        if metrics_df is not None and not metrics_df.empty:
            # ... (之前修复好的 metrics 处理逻辑) ...
            # ... 确保在 for 循环内部，有 if symbol in valid_symbols_whitelist 的判断 ...

    except Exception as e:
        print(f"An unhandled error occurred: {e}"); sys.exit(1)
    finally:
        print("\n--- Daily Full Data Update Job FINISHED ---")

if __name__ == '__main__':
    do_update_job()
