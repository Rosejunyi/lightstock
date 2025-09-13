# scripts/calculate_indicators.py (最终融合版 - 基于成功模式重构)
import os
import sys
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

# ===> [核心原则] 从成功代码中提取的核心参数 <===
STRATEGY_PERIOD = 120 
BATCH_SIZE = 100 # 使用一个经过验证的、更安全的批次大小
print(f"\n!!! RUNNING IN FINAL FUSION MODE (Batch Size: {BATCH_SIZE}, Period: {STRATEGY_PERIOD}) !!!\n")

def main():
    print("--- Starting Job: [3/3] Calculate All Indicators (Final Fusion Engine) ---")
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found."); sys.exit(1)
        
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print("\n--- Step 1: Determining target calculation date ---")
    try:
        latest_bar_response = supabase.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
        if not latest_bar_response.data:
            print("Error: No data in daily_bars table. Exiting."); sys.exit(1)
        
        target_date_str = latest_bar_response.data[0]['date']
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        print(f"  -> Target date for calculation is: {target_date_str}")
    except Exception as e:
        print(f"  -> Error fetching latest date: {e}. Exiting."); sys.exit(1)

    print("\n--- Step 2: Preparing symbol list ---")
    try:
        response = supabase.table('stocks_info').select('symbol').execute()
        all_symbols = [item['symbol'] for item in response.data]
        print(f"  -> Found {len(all_symbols)} symbols for calculation.")
    except Exception as e:
        print(f"  -> Error fetching symbol list: {e}. Exiting."); sys.exit(1)

    # ===> [核心重构] 我们将所有计算都放在一个循环内，并最后统一上传 <===
    all_records_to_upsert = []
    total_batches = (len(all_symbols) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(all_symbols), BATCH_SIZE):
        batch_symbols = all_symbols[i:i+BATCH_SIZE]
        current_batch_num = i//BATCH_SIZE + 1
        print(f"\n--- Processing batch {current_batch_num}/{total_batches} ---")
        
        try:
            # 1. 为当前批次获取足够的历史数据
            start_date_batch = (target_date - timedelta(days=STRATEGY_PERIOD + 60)).strftime('%Y-%m-%d')
            response = supabase.table('daily_bars') \
                .select('symbol, date, open, high, low, close, volume') \
                .in_('symbol', batch_symbols) \
                .gte('date', start_date_batch) \
                .lte('date', target_date_str) \
                .order('date', desc=False) \
                .execute()
            
            if not response.data:
                print("  -> No historical data for this batch. Skipping.")
                continue
            
            df_batch = pd.DataFrame(response.data)
            df_batch['date'] = pd.to_datetime(df_batch['date']).dt.date
            
            # 2. 定义统一计算函数，并采用成功代码中的 "return None" 健壮模式
            def calculate_all_indicators(group):
                # 至少需要30天数据来保证基础指标有意义
                if len(group) < 30: return None
                
                CLOSE = group['close']; HIGH = group['high']; LOW = group['low']; VOLUME = group['volume']
                
                group['ma10'] = MA(CLOSE, 10); group['ma20'] = MA(CLOSE, 20); group['ma50'] = MA(CLOSE, 50)
                group['ma60'] = MA(CLOSE, 60)
                
                # 只有当数据足够长时，才计算长周期指标
                if len(group) >= STRATEGY_PERIOD:
                    group['ma150'] = MA(CLOSE, 150)
                    group['high_52w'] = HIGH.rolling(window=STRATEGY_PERIOD, min_periods=1).max()
                    group['low_52w'] = LOW.rolling(window=STRATEGY_PERIOD, min_periods=1).min()

                group['volume_ma10'] = VOLUME.rolling(window=10).mean(); group['volume_ma30'] = VOLUME.rolling(window=30).mean()
                group['volume_ma60'] = VOLUME.rolling(window=60).mean(); group['volume_ma90'] = VOLUME.rolling(window=90).mean()
                
                DIF, DEA, _ = MACD(CLOSE.values); group['macd_diff'] = DIF; group['macd_dea'] = DEA
                group['rsi14'] = RSI(CLOSE.values, 14)
                return group

            df_with_ta = df_batch.groupby('symbol', group_keys=False).apply(calculate_all_indicators)
            
            # 采用成功代码中的 ".dropna()" 清洗模式
            df_with_ta.dropna(subset=['ma10'], inplace=True) 
            
            if df_with_ta.empty:
                print("  -> No stocks in this batch had enough data for calculation.")
                continue

            # 3. 筛选出目标日期的结果
            today_indicators = df_with_ta[df_with_ta['date'] == target_date].copy()
            
            # 4. 准备本批次的数据
            indicator_columns = ['ma10', 'ma20', 'ma50', 'ma60', 'ma150', 'high_52w', 'low_52w', 'volume_ma10', 'volume_ma30', 'volume_ma60', 'volume_ma90', 'macd_diff', 'macd_dea', 'rsi14']
            for index, row in today_indicators.iterrows():
                record = {'symbol': row['symbol'], 'date': row['date'].strftime('%Y-%m-%d')}
                for col in indicator_columns:
                    if col in row and pd.notna(row[col]):
                        record[col] = float(row[col])
                if len(record) > 2: # 确保除了 symbol 和 date 之外至少有一个指标
                    all_records_to_upsert.append(record)

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"  -> An error occurred processing batch {current_batch_num}. Skipping.")
    
    # ===> [核心回归] 所有批次处理完后，进行一次性的数据上传 <===
    if all_records_to_upsert:
        print(f"\n--- Step 3: Upserting all calculated indicators ---")
        print(f"  -> Preparing to upsert a total of {len(all_records_to_upsert)} records...")
        
        # 为了应对 Supabase 可能的单次上传限制，我们再次分批上传
        for i in range(0, len(all_records_to_upsert), BATCH_SIZE):
            upload_batch = all_records_to_upsert[i:i+BATCH_SIZE]
            print(f"    -> Upserting chunk {i//BATCH_SIZE + 1} with {len(upload_batch)} records...")
            supabase.table('daily_metrics').upsert(upload_batch, on_conflict='symbol,date').execute()
        
        print("  -> All indicators updated successfully!")
    else:
        print("\n--- No new indicators were calculated to be upserted. ---")
        
    print("\n--- Job Finished: Calculate All Indicators ---")

if __name__ == '__main__':
    main()
