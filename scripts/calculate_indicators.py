# scripts/calculate_indicators.py (最终的、全功能、分批健壮版)
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

    batch_size = 100 # 每次处理 100 只股票，在性能和稳定性之间取得平衡
    total_batches = (len(symbols_to_process) + batch_size - 1) // batch_size
    all_records_to_upsert = []

    for i in range(0, len(symbols_to_process), batch_size):
        batch_symbols = symbols_to_process[i:i+batch_size]
        current_batch_num = i//batch_size + 1
        print(f"\n--- Processing indicator batch {current_batch_num}/{total_batches} ---")
        
        try:
            # 1. 为当前批次，获取完整的历史数据 (需要约一年数据来计算 MA200 和 52周高低)
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
                
                # 安全检查，确保有足够数据
                if len(CLOSE) < 250: return None # 52周约250个交易日

                # --- 核心计算：计算所有需要的指标 ---
                group['change_percent'] = (CLOSE / REF(CLOSE, 1) - 1) * 100
                VOL_MA5 = MA(VOL, 5)
                group['volume_ratio_5d'] = np.where(VOL_MA5 > 0, VOL / VOL_MA5, np.nan)
                group['change_percent_10d'] = (CLOSE / REF(CLOSE, 10) - 1) * 100
                group['ma5'] = MA(CLOSE, 5); group['ma10'] = MA(CLOSE, 10); group['ma20'] = MA(CLOSE, 20); group['ma60'] = MA(CLOSE, 60);
                group['ma50'] = MA(CLOSE, 50); group['ma150'] = MA(CLOSE, 150); group['ma200'] = MA(CLOSE, 200)
                DIF, DEA, _ = MACD(CLOSE); group['macd_diff'] = DIF; group['macd_dea'] = DEA
                K, D, J = KDJ(CLOSE, HIGH, LOW); group['kdj_k'] = K; group['kdj_d'] = D; group['kdj_j'] = J
                group['rsi14'] = RSI(CLOSE, 14)
                group['high_52w'] = HHV(HIGH, 250)
                group['low_52w'] = LLV(LOW, 250)
                group['volume_ma10'] = MA(VOL, 10); group['volume_ma30'] = MA(VOL, 30)
                group['volume_ma60'] = MA(VOL, 60); group['volume_ma90'] = MA(VOL, 90)
                group['change_percent_30d'] = (CLOSE / REF(CLOSE, 30) - 1) * 100
                group['change_percent_60d'] = (CLOSE / REF(CLOSE, 60) - 1) * 100
                # ... 你可以继续在这里添加更多 MyTT 指标 ...
                return group
            
            # 3. 分组计算
            df_with_ta = df.groupby('symbol', group_keys=False).apply(calculate_all_mytt)
            df_with_ta.dropna(subset=['ma200'], inplace=True) # 以最长周期的指标为准，过滤计算失败的行
            
            if df_with_ta.empty:
                print("  -> No stocks in this batch had enough data for calculation.")
                continue

            # 4. 筛选出目标日期的结果
            today_indicators = df_with_ta[df_with_ta['date'] == target_date_str].copy()
            
            # 5. 准备上传
            # 这是我们 daily_metrics 表里所有需要计算的列
            indicator_columns = [
                'change_percent', 'volume_ratio_5d', 'change_percent_10d', 'ma5', 'ma10', 'ma20', 'ma60',
                'macd_diff', 'macd_dea', 'kdj_k', 'kdj_d', 'kdj_j', 'rsi14', 'ma50', 'ma150', 'ma200',
                'high_52w', 'low_52w', 'volume_ma10', 'volume_ma30', 'volume_ma60', 'volume_ma90',
                'change_percent_30d', 'change_percent_60d'
            ]
            for index, row in today_indicators.iterrows():
                record = {'symbol': row['symbol'], 'date': row['date']}
                for col in indicator_columns:
                    if col in row and pd.notna(row[col]):
                        record[col] = float(row[col])
                
                # 只有当至少有一个指标被成功计算出来时才添加
                if len(record) > 2:
                    all_records_to_upsert.append(record)

        except Exception as e:
            print(f"  -> An error occurred processing batch {current_batch_num}: {e}")
    
    # 6. 在所有批次都处理完后，分批上传
    if all_records_to_upsert:
        print(f"\nUpserting a total of {len(all_records_to_upsert)} records with calculated indicators...")
        upsert_batch_size = 500
        for i in range(0, len(all_records_to_upsert), upsert_batch_size):
            batch = all_records_to_upsert[i:i+upsert_batch_size]
            supabase.table('daily_metrics').upsert(batch, on_conflict='symbol,date').execute()
        print("  -> All technical and derived indicators updated successfully!")
        
    print("--- Job Finished: Calculate All Indicators ---")

if __name__ == '__main__':
    main()
