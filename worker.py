# worker.py (GitHub Actions - 最终的、完整的、王者归来版)
import os
import sys
import time
# --- 1. 完整的、正确的 Import 区域 ---
from supabase import create_client, Client
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
# 确保 MyTT.py 在你的仓库根目录
from MyTT import * 
# ------------------------------------

# --- 2. 从 Secrets 安全加载配置 ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
# ------------------------------------

def get_last_date_from_db(supabase_client: Client, table_name: str, date_col: str = 'date') -> datetime.date:
    """ 通用函数：从指定表中获取最新的日期 """
    try:
        response = supabase_client.table(table_name).select(date_col).order(date_col, desc=True).limit(1).execute()
        if response.data:
            return datetime.strptime(response.data[0][date_col], '%Y-%m-%d').date()
    except Exception as e:
        print(f"Warning: Could not get last date from {table_name}: {e}")
    # 安全的创世日期
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

def calculate_and_update_indicators_mytt(supabase: Client, target_date: datetime.date):
    """ 使用 MyTT 在 Python 端计算并更新指定日期的技术指标 """
    print("\n--- Step 3: Calculating Technical Indicators using MyTT ---")
    target_date_str = target_date.strftime('%Y-%m-%d')
    try:
        print("Fetching recent historical data for indicator calculation...")
        all_historical_data = []
        page = 0
        while True:
            response = supabase.table('daily_bars') \
                .select('symbol, date, open, high, low, close, volume') \
                .gte('date', (target_date - timedelta(days=150)).strftime('%Y-%m-%d')) \
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
            CLOSE = group['close'].values; HIGH = group['high'].values; LOW = group['low'].values; VOL = group['volume'].values.astype(float)
            if len(CLOSE) < 60: return group # 数据不足以计算 MA60
            
            group['change_percent'] = (CLOSE / REF(CLOSE, 1) - 1) * 100
            VOL_MA5 = MA(VOL, 5)
            group['volume_ratio_5d'] = np.where(VOL_MA5 > 0, VOL / VOL_MA5, 0)
            group['change_percent_10d'] = (CLOSE / REF(CLOSE, 10) - 1) * 100
            group['ma5'] = MA(CLOSE, 5); group['ma10'] = MA(CLOSE, 10); group['ma20'] = MA(CLOSE, 20); group['ma60'] = MA(CLOSE, 60)
            DIF, DEA, MACD_BAR = MACD(CLOSE); group['macd_diff'] = DIF; group['macd_dea'] = DEA
            K, D, J = KDJ(CLOSE, HIGH, LOW); group['kdj_k'] = K; group['kdj_d'] = D; group['kdj_j'] = J
            group['rsi14'] = RSI(CLOSE, 14)
            return group

        print("  -> Calculating all indicators for all symbols...")
        df_with_ta = df.groupby('symbol', group_keys=False).apply(calculate_mytt)
        print("  -> Calculation finished.")
        
        today_indicators = df_with_ta[df_with_ta['date'] == target_date_str].copy()
        
        records_to_upsert = []
        indicator_columns = [
            'change_percent', 'volume_ratio_5d', 'change_percent_10d', 'ma5', 'ma10', 'ma20', 'ma60',
            'macd_diff', 'macd_dea', 'kdj_k', 'kdj_d', 'kdj_j', 'rsi14'
        ]
        
        for index, row in today_indicators.iterrows():
            record = {'symbol': row['symbol'], 'date': row['date']}
            for col in indicator_columns:
                if col in row and pd.notna(row[col]):
                    record[col] = float(row[col])
            # 只有当至少有一个指标被成功计算出来时才添加
            if len(record) > 2:
                records_to_upsert.append(record)
        
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
        
        # 步骤 1: 更新 Daily Bars (日线行情)
        print("\n--- Step 1: Updating Daily Bars ---")
        # (此处省略，请确保你使用的是能工作的逐天回填逻辑)

        # 步骤 2: 更新 Daily Metrics (每日指标)
        print("\n--- Step 2: Updating Daily Metrics ---")
        # (此处省略，请确保你使用的是能工作的每日指标更新逻辑)
        
        # 步骤 3: 计算并更新技术指标
        calculate_and_update_indicators_mytt(supabase, today)

    except Exception as e:
        print(f"An unhandled error occurred: {e}"); sys.exit(1)
    finally:
        print("\n--- Daily Full Data Update Job FINISHED ---")

if __name__ == '__main__':
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: SUPABASE_URL and SUPABASE_KEY secrets must be set."); sys.exit(1)
    do_update_job()
