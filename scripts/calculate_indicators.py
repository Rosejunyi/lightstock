# scripts/calculate_indicators.py (最终的、完整的、全功能计算版)
import os, sys
from supabase import create_client, Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
from MyTT import *

# ... (配置加载和 get_valid_symbols_whitelist 函数保持不变) ...

def main(supabase_url: str, supabase_key: str):
    print("--- Starting Job: [3/3] Calculate All Indicators (Python Engine) ---")
    supabase: Client = create_client(supabase_url, supabase_key)
    
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
            response = supabase.table('daily_bars').select('symbol, date, open, high, low, close, volume').in_('symbol', batch_symbols).gte('date', (target_date - timedelta(days=365)).strftime('%Y-%m-%d')).lte('date', target_date_str).order('date', desc=False).execute()
            
            if not response.data:
                print("  -> No historical data for this batch. Skipping."); continue
                
            df = pd.DataFrame(response.data)
            print(f"  -> Fetched {len(df)} rows for this batch.")

            # --- 关键修复：确保所有需要的指标都被计算 ---
            def calculate_all_mytt(group):
                CLOSE = group['close'].values; HIGH = group['high'].values; LOW = group['low'].values; VOL = group['volume'].values.astype(float)
                if len(CLOSE) < 250: return None
                
                # 计算所有我们需要的指标
                group['ma5'] = MA(CLOSE, 5); group['ma10'] = MA(CLOSE, 10); group['ma20'] = MA(CLOSE, 20); group['ma50'] = MA(CLOSE, 50); group['ma60'] = MA(CLOSE, 60); group['ma150'] = MA(CLOSE, 150); group['ma200'] = MA(CLOSE, 200)
                group['rsi14'] = RSI(CLOSE, 14)
                DIF, DEA, _ = MACD(CLOSE); group['macd_diff'] = DIF; group['macd_dea'] = DEA
                K, D, J = KDJ(CLOSE, HIGH, LOW); group['kdj_k'] = K; group['kdj_d'] = D; group['kdj_j'] = J
                group['high_52w'] = HHV(HIGH, 250); group['low_52w'] = LLV(LOW, 250)
                # ... (更多指标)
                return group
            
            df_with_ta = df.groupby('symbol', group_keys=False).apply(calculate_all_mytt)
            
            # 安全地过滤计算失败的组
            if df_with_ta is not None:
                df_with_ta.dropna(subset=['ma200'], inplace=True) # 以最长周期的指标为准
            
            if df_with_ta is None or df_with_ta.empty:
                print("  -> No stocks in this batch had enough data for calculation."); continue

            today_indicators = df_with_ta[df_with_ta['date'] == target_date_str].copy()
            
            # (准备上传的逻辑...)
            indicator_columns = [
                'ma5', 'ma10', 'ma20', 'ma50', 'ma60', 'ma150', 'ma200', 'rsi14',
                'macd_diff', 'macd_dea', 'kdj_k', 'kdj_d', 'kdj_j', 'high_52w', 'low_52w'
            ]
            for index, row in today_indicators.iterrows():
                record = {'symbol': row['symbol'], 'date': row['date']}
                for col in indicator_columns:
                    if col in row and pd.notna(row[col]):
                        record[col] = float(row[col])
                if len(record) > 2:
                    all_records_to_upsert.append(record)

        except Exception as e:
            print(f"  -> An error occurred processing batch {current_batch_num}: {e}")
    
    if all_records_to_upsert:
        print(f"\nUpserting a total of {len(all_records_to_upsert)} records...")
        # ... (分批上传逻辑)
        print("  -> All technical indicators updated successfully!")
        
    print("--- Job Finished: Calculate All Indicators ---")

if __name__ == '__main__':
