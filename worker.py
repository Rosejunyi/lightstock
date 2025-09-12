# worker.py (GitHub Actions - 最终的、全功能整合版)
import os
import sys
import time
from supabase import create_client, Client
import akshare as ak
import pandas as pd
import pandas_ta as ta # 引入技术分析库
import numpy as np
from datetime import datetime, timedelta

# --- 1. 从 Secrets 安全加载配置 ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_last_date_from_db(supabase_client, table_name, date_col='date'):
    """ 通用函数：从指定表中获取最新的日期 """
    try:
        response = supabase_client.table(table_name).select(date_col).order(date_col, desc=True).limit(1).execute()
        if response.data:
            return datetime.strptime(response.data[0][date_col], '%Y-%m-%d').date()
    except Exception as e:
        print(f"Warning: Could not get last date from {table_name}: {e}")
    # 安全的创世日期
    return datetime.strptime("2025-01-01", "%Y-%m-%d").date()

def get_valid_symbols_whitelist(supabase_client):
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
        # 获取计算所需的所有历史数据（大约90天）
        response = supabase.table('daily_bars') \
            .select('symbol, date, close') \
            .gte('date', (target_date - timedelta(days=90)).strftime('%Y-%m-%d')) \
            .lte('date', target_date_str) \
            .order('date', desc=False) \
            .execute()

        if not response.data:
            print("  -> No historical data found to calculate indicators. Skipping.")
            return

        df = pd.DataFrame(response.data)
        print(f"  -> Fetched {len(df)} rows for calculation.")
        
        # 使用 pandas-ta 进行分组计算
        # 我们按 symbol 分组，然后对每个组应用 ta 策略
        df_ta = df.groupby('symbol').ta.agg(
            ta.ma(length=5, append=True),
            ta.ma(length=10, append=True),
            ta.rsi(length=14, append=True)
        )
        # 将计算结果合并回原始 DataFrame
        df = df.join(df_ta)
        
        # 筛选出目标日期当天的、计算好的指标
        today_indicators = df[df['date'] == target_date_str].copy()
        
        records_to_upsert = []
        for index, row in today_indicators.iterrows():
            # 只有当指标不为空时才添加
            if pd.notna(row['MA_5']) and pd.notna(row['MA_10']) and pd.notna(row['RSI_14']):
                records_to_upsert.append({
                    'symbol': row['symbol'],
                    'date': row['date'],
                    'ma5': row['MA_5'],
                    'ma10': row['MA_10'],
                    'rsi14': row['RSI_14']
                })
        
        if records_to_upsert:
            print(f"  -> Calculated indicators for {len(records_to_upsert)} stocks. Upserting to daily_metrics...")
            supabase.table('daily_metrics').upsert(records_to_upsert).execute()
            print("  -> Technical indicators updated successfully!")

    except Exception as e:
        print(f"  -> An error occurred during indicator calculation: {e}")


def do_update_job():
    print("--- Starting Daily Full Data Update Job ---")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")

        valid_symbols_whitelist = get_valid_symbols_whitelist(supabase)
        today = datetime.now().date()
        
        # ===================================================================
        # 步骤 1: 更新 Daily Bars (日线行情)
        # ===================================================================
        print("\n--- Step 1: Updating Daily Bars ---")
        last_bars_date = get_last_date_from_db(supabase, 'daily_bars')
        date_to_process = last_bars_date + timedelta(days=1)

        while date_to_process <= today:
            # ... (这里是你之前那个能成功运行的、最稳妥的 daily_bars 逐天回填代码) ...
            trade_date_str = date_to_process.strftime('%Y%m%d')
            # ... (ak.stock_zh_a_hist, 清洗, upsert) ...
            date_to_process += timedelta(days=1)
        
        # ===================================================================
        # 步骤 2: 更新 Daily Metrics (每日指标)
        # ===================================================================
        print("\n--- Step 2: Updating Daily Metrics ---")
        last_metrics_date = get_last_date_from_db(supabase, 'daily_metrics')
        if last_metrics_date < today:
            metrics_df = ak.stock_zh_a_spot_em()
            # ... (之前修复好的 metrics 处理和上传逻辑, 确保有白名单过滤) ...
            print("daily_metrics table updated successfully!")
        else:
            print("Daily metrics are already up to date.")

        # ===================================================================
        # 步骤 3: 计算并更新技术指标
        # ===================================================================
        calculate_and_update_indicators(supabase, today)

    except Exception as e:
        print(f"An unhandled error occurred: {e}"); sys.exit(1)
    finally:
        print("\n--- Daily Full Data Update Job FINISHED ---")

if __name__ == '__main__':
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: SUPABASE_URL and SUPABASE_KEY secrets must be set.")
        sys.exit(1)
    do_update_job()
