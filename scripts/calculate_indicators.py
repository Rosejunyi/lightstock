# scripts/calculate_indicators.py (纯 Python 计算引擎版 - 推荐)
import os
import sys
import time
from supabase import create_client, Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
from MyTT import *

# --- 1. 配置加载 ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
# --------------------

def main():
    print("--- Starting Job: [3/3] Calculate All Indicators (Pure Python Engine) ---")
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found."); sys.exit(1)
        
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # -------------------------------------------------------------------------
    # 步骤 1: 确定要计算的目标日期 (获取 daily_bars 中最新的日期)
    # -------------------------------------------------------------------------
    print("\n--- Step 1: Determining target calculation date ---")
    try:
        latest_bar_response = supabase.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
        if not latest_bar_response.data:
            print("Error: No data in daily_bars table. Exiting."); sys.exit(1)
        target_date = datetime.strptime(latest_bar_response.data[0]['date'], '%Y-%m-%d').date()
        target_date_str = target_date.strftime('%Y-%m-%d')
        print(f"  -> Target date for calculation is: {target_date_str}")
    except Exception as e:
        print(f"  -> Error fetching latest date: {e}. Exiting."); sys.exit(1)

    # ===================================================================
    # 步骤 2: 在 Python 中计算所有指标 (简单 + 复杂)
    # ===================================================================
    print("\n--- Step 2: Calculating all indicators in Python ---")
    try:
        # a. 获取所有股票列表
        print("  Fetching whitelist from stocks_info...")
        response = supabase.table('stocks_info').select('symbol').execute()
        symbols_to_process = [item['symbol'] for item in response.data]
        print(f"  -> Found {len(symbols_to_process)} symbols for calculation.")

        # b. 分批处理
        batch_size = 200 # 这个值可以根据 Action 的内存进行微调
        total_batches = (len(symbols_to_process) + batch_size - 1) // batch_size

        for i in range(0, len(symbols_to_process), batch_size):
            batch_symbols = symbols_to_process[i:i+batch_size]
            current_batch_num = i//batch_size + 1
            print(f"\n  --- Processing indicator batch {current_batch_num}/{total_batches} ---")
            
            # c. 为当前批次获取足够的历史数据 (MA200 需要约 300 天数据)
            response = supabase.table('daily_bars') \
                .select('symbol, date, open, high, low, close, volume') \
                .in_('symbol', batch_symbols) \
                .gte('date', (target_date - timedelta(days=300)).strftime('%Y-%m-%d')) \
                .lte('date', target_date_str) \
                .order('date', desc=False) \
                .execute()
            
            if not response.data:
                print("    -> No historical data for this batch. Skipping.")
                continue
            
            df = pd.DataFrame(response.data)
            
            # d. 定义一个统一的计算函数 (简单 + 复杂)
            def calculate_all_indicators(group):
                CLOSE = group['close']
                HIGH = group['high']
                LOW = group['low']
                VOLUME = group['volume']

                # --- 计算简单移动平均线 (MA) ---
                group['ma5'] = MA(CLOSE, 5)
                group['ma10'] = MA(CLOSE, 10)
                group['ma20'] = MA(CLOSE, 20)
                group['ma60'] = MA(CLOSE, 60)
                group['ma200'] = MA(CLOSE, 200)

                # --- 计算变化率 (change_percent) ---
                group['change_percent'] = (CLOSE / REF(CLOSE, 1) - 1) * 100

                # --- 计算复杂指标 (MACD, KDJ, RSI) using MyTT ---
                if len(CLOSE) >= 30: # 确保有足够数据
                    DIF, DEA, _ = MACD(CLOSE.values); group['macd_diff'] = DIF; group['macd_dea'] = DEA
                    K, D, J = KDJ(CLOSE.values, HIGH.values, LOW.values); group['kdj_k'] = K; group['kdj_d'] = D; group['kdj_j'] = J
                    group['rsi14'] = RSI(CLOSE.values, 14)
                
                return group

            df_with_ta = df.groupby('symbol', group_keys=False).apply(calculate_all_indicators)
            
            # e. 筛选出目标日期的结果
            today_indicators = df_with_ta[df_with_ta['date'] == target_date_str].copy()
            
            # f. 准备并上传
            records_to_upsert = []
            # 定义所有需要上传的指标列
            indicator_columns = [
                'ma5', 'ma10', 'ma20', 'ma60', 'ma200', 'change_percent',
                'macd_diff', 'macd_dea', 'kdj_k', 'kdj_d', 'kdj_j', 'rsi14'
            ]
            for index, row in today_indicators.iterrows():
                record = {'symbol': row['symbol'], 'date': row['date']}
                for col in indicator_columns:
                    # 确保列存在且值不是 NaN
                    if col in row and pd.notna(row[col]):
                        # 将 numpy 类型转换为 python 内置类型
                        record[col] = float(row[col])
                # 只有当计算出了至少一个指标时才添加
                if len(record) > 2:
                    records_to_upsert.append(record)
            
            if records_to_upsert:
                print(f"    -> Calculated indicators for {len(records_to_upsert)} stocks. Upserting...")
                supabase.table('daily_metrics').upsert(records_to_upsert, on_conflict='symbol,date').execute()

    except Exception as e:
        print(f"  -> An error occurred during Python calculation: {e}")
        # 在 GitHub Actions 中，非零退出码会标记为失败
        sys.exit(1)
        
    print("\n--- Job Finished: Calculate All Indicators ---")

if __name__ == '__main__':
    main()
