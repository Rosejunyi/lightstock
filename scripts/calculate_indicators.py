# scripts/calculate_indicators.py (最终的、纯 MyTT 版)
import os, sys
from supabase import create_client, Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
# 确保 MyTT.py 在同一个 scripts 文件夹内
from MyTT import *

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_valid_symbols_whitelist(supabase_client: Client) -> list:
    # ... (这个函数内容是正确的，保持不变) ...

def main(supabase_url: str, supabase_key: str):
    print("--- Starting Job: [3/3] Calculate All Indicators using MyTT ---")
    supabase: Client = create_client(supabase_url, supabase_key)
    
    target_date = datetime.now().date()
    target_date_str = target_date.strftime('%Y-%m-%d')
    
    symbols_to_process = get_valid_symbols_whitelist(supabase)
    if not symbols_to_process: return

    # --- 分批处理 ---
    batch_size = 100
    all_records_to_upsert = []
    for i in range(0, len(symbols_to_process), batch_size):
        batch_symbols = symbols_to_process[i:i+batch_size]
        print(f"\n--- Processing indicator batch {i//batch_size + 1} ---")
        
        # 1. 为当前批次，获取完整的历史数据
        response = supabase.table('daily_bars').select('symbol, date, close, high, low, volume') \
            .in_('symbol', batch_symbols) \
            .gte('date', (target_date - timedelta(days=365)).strftime('%Y-%m-%d')) \
            .lte('date', target_date_str) \
            .order('date', desc=False).execute()
            
        if not response.data: continue
        df = pd.DataFrame(response.data)

        # 2. 定义并应用 MyTT 计算函数
        def calculate_mytt(group):
            CLOSE = group['close'].values; HIGH = group['high'].values; LOW = group['low'].values
            if len(CLOSE) < 200: return None
            
            group['ma5'] = MA(CLOSE, 5)
            group['ma10'] = MA(CLOSE, 10)
            group['ma200'] = MA(CLOSE, 200)
            group['rsi14'] = RSI(CLOSE, 14)
            # ... (在这里添加所有其他需要的 MyTT 指标计算)
            return group

        df_with_ta = df.groupby('symbol', group_keys=False).apply(calculate_mytt)
        if df_with_ta is not None: df_with_ta.dropna(inplace=True)
        if df_with_ta is None or df_with_ta.empty: continue
            
        today_indicators = df_with_ta[df_with_ta['date'] == target_date_str].copy()
        
        # 3. 准备上传
        for index, row in today_indicators.iterrows():
            record = {
                'symbol': row['symbol'], 'date': row['date'],
                'ma5': float(row['ma5']),
                'ma10': float(row['ma10']),
                'ma200': float(row['ma200']),
                'rsi14': float(row['rsi14'])
            }
            all_records_to_upsert.append(record)

    if all_records_to_upsert:
        print(f"\nUpserting {len(all_records_to_upsert)} records with calculated indicators...")
        supabase.table('daily_metrics').upsert(all_records_to_upsert, on_conflict='symbol,date').execute()
        print("Technical indicators updated successfully!")
        
    print("--- Job Finished ---")

if __name__ == '__main__':
    main(SUPABASE_URL, SUPABASE_KEY)
