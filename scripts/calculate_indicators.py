# scripts/calculate_indicators.py (最终的、纯 Python、分批健壮版)
import os, sys
from supabase import create_client, Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
from MyTT import *

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_valid_symbols_whitelist(supabase_client: Client) -> list:
    print("Fetching whitelist from stocks_info (with pagination)...")
    all_symbols = []
    page = 0
    while True:
        response = supabase_client.table('stocks_info').select('symbol').range(page * 1000, (page + 1) * 1000 - 1).execute()
        if not response.data: break
        all_symbols.extend(item['symbol'] for item in response.data)
        if len(response.data) < 1000: break
        page += 1
    print(f"  -> Whitelist created with {len(all_symbols)} symbols.")
    return all_symbols

def main():
    print("--- Starting Job: [3/3] Calculate All Indicators (Python Engine) ---")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    target_date = datetime.now().date()
    target_date_str = target_date.strftime('%Y-%m-%d')
    
    symbols_to_process = get_valid_symbols_whitelist(supabase)
    if not symbols_to_process: return

    # --- 核心：分批处理 ---
    batch_size = 100 # 每次只处理 100 只股票，确保下载和计算都不会超时
    total_batches = (len(symbols_to_process) + batch_size - 1) // batch_size
    all_records_to_upsert = []

    for i in range(0, len(symbols_to_process), batch_size):
        batch_symbols = symbols_to_process[i:i+batch_size]
        current_batch_num = i//batch_size + 1
        print(f"\n--- Processing indicator batch {current_batch_num}/{total_batches} ---")
        
        try:
            # 1. 为当前批次，获取完整的历史数据
            response = supabase.table('daily_bars') \
                .select('symbol, date, open, high, low, close, volume') \
                .in_('symbol', batch_symbols) \
                .gte('date', (target_date - timedelta(days=365)).strftime('%Y-%m-%d')) \
                .lte('date', target_date_str) \
                .order('date', desc=False) \
                .execute()
            
            if not response.data:
                print("  -> No historical data for this batch. Skipping.")
                continue
                
            df = pd.DataFrame(response.data)
            print(f"  -> Fetched {len(df)} rows for this batch.")

            # 2. 定义 MyTT 计算函数
            def calculate_all_mytt(group):
                CLOSE = group['close'].values; HIGH = group['high'].values; LOW = group['low'].values; VOL = group['volume'].values.astype(float)
                if len(CLOSE) < 200: return None # 确保有足够数据计算 MA200
                
                group['ma50'] = MA(CLOSE, 50); group['ma150'] = MA(CLOSE, 150); group['ma200'] = MA(CLOSE, 200)
                # ... (此处省略所有 MyTT 指标计算) ...
                return group
            
            # 3. 分组计算
            df_with_ta = df.groupby('symbol', group_keys=False).apply(calculate_all_mytt)
            df_with_ta.dropna(subset=['ma200'], inplace=True) # 过滤计算失败的行
            
            if df_with_ta.empty:
                print("  -> No stocks in this batch had enough data for calculation.")
                continue

            today_indicators = df_with_ta[df_with_ta['date'] == target_date_str].copy()
            
            # (此处省略准备 records_to_upsert 的逻辑)
            
            if today_indicators.empty == False:
                 all_records_to_upsert.extend(today_indicators.to_dict('records'))

        except Exception as e:
            print(f"  -> An error occurred processing batch {current_batch_num}: {e}")
    
    # 6. 在所有批次都处理完后，分批上传
    if all_records_to_upsert:
        print(f"\nUpserting a total of {len(all_records_to_upsert)} records...")
        upsert_batch_size = 500
        for i in range(0, len(all_records_to_upsert), upsert_batch_size):
            batch = all_records_to_upsert[i:i+upsert_batch_size]
            supabase.table('daily_metrics').upsert(batch, on_conflict='symbol,date').execute()
        print("  -> All technical indicators updated successfully!")
        
    print("--- Job Finished: Calculate All Indicators ---")

if __name__ == '__main__':
    main()
