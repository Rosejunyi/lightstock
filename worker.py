# worker.py (GitHub Actions - 最终的、完整的、王者归来版)
import os
import sys
import time
# --- 关键修复：同时导入 Client ---
from supabase import create_client, Client
import akshare as ak
import pandas as pd
import pandas_ta as ta
import numpy as np
from datetime import datetime, timedelta

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_last_date_from_db(supabase_client: Client, table_name: str, date_col: str = 'date') -> datetime.date:
    """ 通用函数：从指定表中获取最新的日期 """
    try:
        response = supabase_client.table(table_name).select(date_col).order(date_col, desc=True).limit(1).execute()
        if response.data:
            return datetime.strptime(response.data[0][date_col], '%Y-%m-%d').date()
    except Exception as e:
        print(f"Warning: Could not get last date from {table_name}: {e}")
    return datetime.strptime("2025-01-01", "%Y-%m-%d").date()

def get_valid_symbols_whitelist(supabase_client: Client) -> set:
    """ 从 stocks_info 分页获取所有有效的股票 symbol 列表 """
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

def calculate_and_update_indicators(supabase: Client, target_date: datetime.date):
    """ 在 Python 端计算并更新指定日期的技术指标 """
    print("\n--- Step 3: Calculating Technical Indicators ---")
    target_date_str = target_date.strftime('%Y-%m-%d')
    try:
        print("Fetching recent historical data for indicator calculation...")
        response = supabase.table('daily_bars') \
            .select('symbol, date, close') \
            .gte('date', (target_date - timedelta(days=90)).strftime('%Y-%m-%d')) \
            .lte('date', target_date_str) \
            .order('date', desc=False) \
            .execute()

        if not response.data:
            print("  -> No historical data found to calculate indicators. Skipping."); return

        df = pd.DataFrame(response.data)
        print(f"  -> Fetched {len(df)} rows for calculation.")
        
        def calculate_ta(group):
            group.ta.rsi(length=14, append=True)
            group.ta.sma(length=5, append=True)
            group.ta.sma(length=10, append=True)
            return group

        df_with_ta = df.groupby('symbol', group_keys=False).apply(calculate_ta)
        
        today_indicators = df_with_ta[df_with_ta['date'] == target_date_str].copy()
        
        records_to_upsert = []
        for index, row in today_indicators.iterrows():
            if pd.notna(row['SMA_5']) and pd.notna(row['SMA_10']) and pd.notna(row['RSI_14']):
                records_to_upsert.append({
                    'symbol': row['symbol'], 'date': row['date'],
                    'ma5': row['SMA_5'], 'ma10': row['SMA_10'], 'rsi14': row['RSI_14']
                })
        
        if records_to_upsert:
            print(f"  -> Calculated indicators for {len(records_to_upsert)} stocks. Upserting to daily_metrics...")
            supabase.table('daily_metrics').upsert(records_to_upsert, on_conflict='symbol,date').execute()
            print("  -> Technical indicators updated successfully!")
        else:
            print("  -> No valid indicators calculated for today.")

    except Exception as e:
        print(f"  -> An error occurred during indicator calculation: {e}")

def do_update_job():
    print("--- Starting Daily Full Data Update Job ---")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        # ... (Step 1: Updating Daily Bars 和 Step 2: Updating Daily Metrics 的逻辑) ...
        # ... (请确保你使用的是能成功运行的、最新的版本) ...
        
        today = datetime.now().date()
        calculate_and_update_indicators(supabase, today)

    except Exception as e:
        print(f"An unhandled error occurred: {e}"); sys.exit(1)
    finally:
        print("\n--- Daily Full Data Update Job FINISHED ---")

if __name__ == '__main__':
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: SUPABASE_URL and SUPABASE_KEY secrets must be set."); sys.exit(1)
    do_update_job()
