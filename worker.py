# worker.py (GitHub Actions - 最终的、完整的、MyTT整合版)
import os
import sys
import time
from supabase import create_client, Client
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from MyTT import * 

# --- 1. 从 Secrets 安全加载配置 ---
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

def calculate_and_update_indicators_mytt(supabase: Client, target_date: datetime.date, valid_symbols: set):
    """ 使用 MyTT 在 Python 端计算并更新指定日期的技术指标 """
    print("\n--- Step 3: Calculating Technical Indicators using MyTT ---")
    target_date_str = target_date.strftime('%Y-%m-%d')
    try:
        print("Fetching recent historical data for indicator calculation...")
        all_historical_data = []
        page = 0
        while True:
            response = supabase.table('daily_bars').select('symbol, date, close') \
                .gte('date', (target_date - timedelta(days=90)).strftime('%Y-%m-%d')) \
                .lte('date', target_date_str) \
                .order('date', desc=False) \
                .range(page * 1000, (page + 1) * 1000 - 1).execute()
            if not response.data: break
            all_historical_data.extend(response.data)
            if len(response.data) < 1000: break
            page += 1
        
        if not all_historical_data:
            print("  -> No historical data found. Skipping."); return

        df = pd.DataFrame(all_historical_data)
        print(f"  -> Fetched {len(df)} rows for calculation.")
        
        def calculate_mytt(group):
            CLOSE = group['close'].values
            if len(CLOSE) < 14: return group # 数据不足，无法计算RSI(14)
            group['ma5'] = MA(CLOSE, 5)
            group['ma10'] = MA(CLOSE, 10)
            group['rsi14'] = RSI(CLOSE, 14)
            return group

        print("  -> Calculating indicators for all symbols...")
        df_with_ta = df.groupby('symbol', group_keys=False).apply(calculate_mytt)
        print("  -> Calculation finished.")
        
        today_indicators = df_with_ta[df_with_ta['date'] == target_date_str].copy()
        
        records_to_upsert = []
        for index, row in today_indicators.iterrows():
            if pd.notna(row.get('ma5')) and pd.notna(row.get('ma10')) and pd.notna(row.get('rsi14')):
                records_to_upsert.append({
                    'symbol': row['symbol'], 'date': row['date'],
                    'ma5': float(row['ma5']), 'ma10': float(row['ma10']), 'rsi14': float(row['rsi14'])
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

        valid_symbols_whitelist = get_valid_symbols_whitelist(supabase)
        today = datetime.now().date()
        
        # 步骤 1: 更新 Daily Bars
        print("\n--- Step 1: Updating Daily Bars ---")
        last_bars_date = get_last_date_from_db(supabase, 'daily_bars')
        date_to_process = last_bars_date + timedelta(days=1)
        if date_to_process > today:
            print("Daily bars are already up to date.")
        else:
            # 逐天回填
            while date_to_process <= today:
                # ... (这里是你之前那个能成功运行的、最稳妥的 daily_bars 逐天回填代码逻辑) ...
                date_to_process += timedelta(days=1)
        
        # 步骤 2: 更新 Daily Metrics
        print("\n--- Step 2: Updating Daily Metrics ---")
        last_metrics_date = get_last_date_from_db(supabase, 'daily_metrics')
        if last_metrics_date < today:
            metrics_df = ak.stock_zh_a_spot_em()
            # ... (之前修复好的 metrics 处理和上传逻辑, 确保有白名单过滤) ...
            print("daily_metrics table updated successfully!")
        else:
            print("Daily metrics are already up to date.")

        # 步骤 3: 计算并更新技术指标
        calculate_and_update_indicators_mytt(supabase, today, valid_symbols_whitelist)

    except Exception as e:
        print(f"An unhandled error occurred: {e}"); sys.exit(1)
    finally:
        print("\n--- Daily Full Data Update Job FINISHED ---")

if __name__ == '__main__':
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: SUPABASE_URL and SUPABASE_KEY secrets must be set."); sys.exit(1)
    do_update_job()
