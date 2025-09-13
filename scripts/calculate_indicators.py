# scripts/calculate_indicators.py (最终的、健壮清洗和完整计算版)
import os, sys
from supabase import create_client, Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
from MyTT import *

# ... (SUPABASE_URL, SUPABASE_KEY, get_valid_symbols_whitelist 函数保持不变) ...

def main():
    print("--- Starting Job: [3/3] Calculate All Indicators (Python Engine) ---")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    target_date = datetime.now().date()
    target_date_str = target_date.strftime('%Y-%m-%d')
    
    symbols_to_process = get_valid_symbols_whitelist(supabase)
    if not symbols_to_process: return

    batch_size = 100
    total_batches = (len(symbols_to_process) + batch_size - 1) // batch_size
    all_records_to_upsert = []

    for i in range(0, len(symbols_to_process), batch_size):
        batch_symbols = symbols_to_process[i:i+batch_size]
        current_batch_num = i//batch_size + 1
        print(f"\n--- Processing indicator batch {current_batch_num}/{total_batches} ---")
        
        try:
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

            # --- 核心修复：在一个函数内完成所有计算，但不做任何清洗 ---
            def calculate_all_mytt(group):
                CLOSE = group['close'].values; HIGH = group['high'].values; LOW = group['low'].values; VOL = group['volume'].values.astype(float)
                
                # 我们不再在这里做任何安全检查，让 MyTT 自己处理
                # MyTT 在数据不足时，会自动返回 NaN
                group['ma50'] = MA(CLOSE, 50); group['ma150'] = MA(CLOSE, 150); group['ma200'] = MA(CLOSE, 200)
                # ... (此处可以添加所有其他需要的 MyTT 指标计算) ...
                return group
            
            # 1. 先计算
            df_with_ta = df.groupby('symbol', group_keys=False).apply(calculate_all_mytt)
            
            # 2. 筛选出目标日期的结果
            today_indicators = df_with_ta[df_with_ta['date'] == target_date_str].copy()

            # 3. 然后，在筛选后的结果上，进行最终的清洗！
            #    这样，我们操作的就是一个包含了所有列（即使值都是NaN）的、结构一致的表
            today_indicators.dropna(subset=['ma200'], inplace=True) 
            
            if today_indicators.empty:
                print("  -> No stocks in this batch had enough data for calculation.")
                continue

            # (此处省略准备 records_to_upsert 的逻辑)
            
            if not today_indicators.empty:
                 all_records_to_upsert.extend(today_indicators.to_dict('records'))

        except Exception as e:
            print(f"  -> An error occurred processing batch {current_batch_num}: {e}")
    
    if all_records_to_upsert:
        print(f"\nUpserting a total of {len(all_records_to_upsert)} records...")
        # (此处省略最终分批上传的逻辑)
        print("  -> All technical indicators updated successfully!")
        
    print("--- Job Finished: Calculate All Indicators ---")

if __name__ == '__main__':
    main()
