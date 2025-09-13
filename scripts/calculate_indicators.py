# scripts/calculate_indicators.py (最终的“混合动力总指挥”版)
import os
import sys
import time
from supabase import create_client, Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
# 确保 MyTT.py 在 scripts 文件夹内
from MyTT import *

# --- 1. 配置加载 ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
# --------------------

def main():
    print("--- Starting Job: [3/3] Calculate All Indicators (Hybrid Engine) ---")
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found."); sys.exit(1)
        
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    today = datetime.now().date()
    today_str = today.strftime('%Y-%m-%d')
    
    # ===================================================================
    # 步骤 1: 命令数据库进行“粗加工”（计算简单指标）
    # ===================================================================
    print(f"\n--- Step 1: Triggering SQL-based calculations for {today_str} ---")
    try:
        # 我们假设你已经在 Supabase 创建了名为 calculate_simple_indicators 的函数
        # 并且 daily_metrics 表已经添加了所有需要的列
        supabase.rpc('run_all_sql_calculations', {'target_date': today_str}).execute()
        print("  -> SQL calculations completed successfully in database.")
    except Exception as e:
        print(f"  -> Error during SQL calculation: {e}")
        # 即使这里失败，我们仍然可以继续尝试 Python 计算
        
    # ===================================================================
    # 步骤 2: 在 Python 中进行“精加工”（计算复杂指标）
    # ===================================================================
    print("\n--- Step 2: Calculating complex indicators in Python using MyTT ---")
    try:
        # a. 获取一个需要进行复杂计算的股票池（可以先从数据库粗筛）
        #    为了简化，我们先假设处理所有股票
        print("  Fetching whitelist from stocks_info...")
        response = supabase.table('stocks_info').select('symbol').execute()
        symbols_to_process = [item['symbol'] for item in response.data]
        print(f"  -> Found {len(symbols_to_process)} symbols for complex calculation.")

        # b. 分批处理
        batch_size = 200
        total_batches = (len(symbols_to_process) + batch_size - 1) // batch_size

        for i in range(0, len(symbols_to_process), batch_size):
            batch_symbols = symbols_to_process[i:i+batch_size]
            current_batch_num = i//batch_size + 1
            print(f"\n  --- Processing complex indicator batch {current_batch_num}/{total_batches} ---")
            
            # c. 只为当前批次，获取完整的历史数据
            response = supabase.table('daily_bars') \
                .select('symbol, date, open, high, low, close, volume') \
                .in_('symbol', batch_symbols) \
                .gte('date', (today - timedelta(days=150)).strftime('%Y-%m-%d')) \
                .lte('date', today_str) \
                .order('date', desc=False) \
                .execute()
            
            if not response.data:
                print("    -> No historical data for this batch. Skipping.")
                continue
            
            df = pd.DataFrame(response.data)
            
            # d. 定义并应用 MyTT 计算函数
            def calculate_mytt_complex(group):
                CLOSE = group['close'].values; HIGH = group['high'].values; LOW = group['low'].values
                if len(CLOSE) < 30: return group # 至少需要30天数据来计算MACD
                DIF, DEA, _ = MACD(CLOSE); group['macd_diff'] = DIF; group['macd_dea'] = DEA
                K, D, J = KDJ(CLOSE, HIGH, LOW); group['kdj_k'] = K; group['kdj_d'] = D; group['kdj_j'] = J
                group['rsi14'] = RSI(CLOSE, 14)
                return group

            df_with_ta = df.groupby('symbol', group_keys=False).apply(calculate_mytt_complex)
            
            # e. 筛选出目标日期的结果
            today_indicators = df_with_ta[df_with_ta['date'] == today_str].copy()
            
            # f. 准备并上传
            records_to_upsert = []
            indicator_columns = ['macd_diff', 'macd_dea', 'kdj_k', 'kdj_d', 'kdj_j', 'rsi14']
            for index, row in today_indicators.iterrows():
                record = {'symbol': row['symbol'], 'date': row['date']}
                for col in indicator_columns:
                    if col in row and pd.notna(row[col]):
                        record[col] = float(row[col])
                if len(record) > 2:
                    records_to_upsert.append(record)
            
            if records_to_upsert:
                print(f"    -> Calculated complex indicators for {len(records_to_upsert)} stocks. Upserting...")
                supabase.table('daily_metrics').upsert(records_to_upsert, on_conflict='symbol,date').execute()

    except Exception as e:
        print(f"  -> An error occurred during Python calculation: {e}")
        
    print("\n--- Job Finished: Calculate All Indicators ---")

if __name__ == '__main__':
    main()
