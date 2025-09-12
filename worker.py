# worker.py (GitHub Actions - 最终的、MyTT 整合版)
import os
import sys
import time
from supabase import create_client, Client
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from MyTT import * # 导入我们刚刚下载的 MyTT 神器！

# --- 1. 从 Secrets 安全加载配置 ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# ... (get_last_trade_date_from_db 和 get_valid_symbols_whitelist 函数保持不变) ...
def get_last_trade_date_from_db(supabase_client: Client, table_name: str, date_col: str = 'date') -> datetime.date:
    # ...
def get_valid_symbols_whitelist(supabase_client: Client) -> set:
    # ...

def calculate_and_update_indicators_mytt(supabase: Client, target_date: datetime.date):
    """ 使用 MyTT 在 Python 端计算并更新指定日期的技术指标 """
    print("\n--- Step 3: Calculating Technical Indicators using MyTT ---")
    target_date_str = target_date.strftime('%Y-%m-%d')
    try:
        print("Fetching recent historical data for indicator calculation...")
        # ... (分页获取所有历史数据的逻辑不变) ...
        # 假设我们已经拿到了一个包含所有股票、最近90天数据的 DataFrame: df
        all_historical_data = []
        page = 0
        while True:
            response = supabase.table('daily_bars').select('symbol, date, close').range(page * 1000, (page + 1) * 1000 - 1).execute()
            if not response.data: break
            all_historical_data.extend(response.data)
            if len(response.data) < 1000: break
            page += 1
        
        if not all_historical_data:
            print("  -> No historical data found. Skipping."); return

        df = pd.DataFrame(all_historical_data)
        print(f"  -> Fetched {len(df)} rows for calculation.")
        
        # --- 核心修复：使用 MyTT 进行分组计算 ---
        def calculate_mytt(group):
            # MyTT 需要的输入是 Numpy 数组
            CLOSE = group['close'].values
            
            # MyTT 函数返回的是 Numpy 数组，长度与输入相同
            group['ma5'] = MA(CLOSE, 5)
            group['ma10'] = MA(CLOSE, 10)
            group['rsi14'] = RSI(CLOSE, 14)
            return group

        print("  -> Calculating indicators for all symbols...")
        # 对每个 symbol 分组，然后应用我们的 MyTT 计算函数
        df_with_ta = df.groupby('symbol', group_keys=False).apply(calculate_mytt)
        print("  -> Calculation finished.")
        # ----------------------------------------------
        
        # 筛选出目标日期当天的、计算好的指标
        today_indicators = df_with_ta[df_with_ta['date'] == target_date_str].copy()
        
        records_to_upsert = []
        for index, row in today_indicators.iterrows():
            # MyTT 的结果是 Numpy 类型，需要转换成 Python 原生类型
            if pd.notna(row['ma5']) and pd.notna(row['ma10']) and pd.notna(row['rsi14']):
                records_to_upsert.append({
                    'symbol': row['symbol'], 'date': row['date'],
                    'ma5': float(row['ma5']),
                    'ma10': float(row['ma10']),
                    'rsi14': float(row['rsi14'])
                })
        
        if records_to_upsert:
            print(f"  -> Found {len(records_to_upsert)} stocks with valid indicators. Upserting...")
            supabase.table('daily_metrics').upsert(records_to_upsert, on_conflict='symbol,date').execute()
            print("  -> Technical indicators updated successfully!")

    except Exception as e:
        print(f"  -> An error occurred during MyTT calculation: {e}")

def do_update_job():
    print("--- Starting Daily Full Data Update Job ---")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        # ... (Step 1: Updating Daily Bars 和 Step 2: Updating Daily Metrics 的逻辑) ...
        # ... (请确保你使用的是能成功运行的、最新的版本) ...
        
        today = datetime.now().date()
        
        # 在所有数据更新成功后，调用我们新的、基于 MyTT 的计算函数
        calculate_and_update_indicators_mytt(supabase, today)

    except Exception as e:
        print(f"An unhandled error occurred: {e}"); sys.exit(1)
    finally:
        print("\n--- Daily Full Data Update Job FINISHED ---")

if __name__ == '__main__':
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: SUPABASE_URL and SUPABASE_KEY secrets must be set."); sys.exit(1)
    do_update_job()
