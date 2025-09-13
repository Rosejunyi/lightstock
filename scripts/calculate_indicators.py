# scripts/calculate_indicators.py (The Masterpiece Edition - Based on your proven success pattern)
import os
import sys
from supabase import create_client, Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
from MyTT import *

# --- 1. Configuration ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# --- 2. Core Strategy Parameters ---
STRATEGY_PERIOD = 120 
BATCH_SIZE_UPSERT = 200 # 定义一个用于上传的安全批次大小
print(f"\n!!! RUNNING THE MASTERPIECE EDITION (Period: {STRATEGY_PERIOD}) !!!\n")

def main():
    print("--- Starting Job: [3/3] Calculate All Indicators ---")
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found."); sys.exit(1)
        
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # --- Step 1: Determine Target Date ---
    try:
        latest_bar_response = supabase.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
        if not latest_bar_response.data:
            print("Error: No data in daily_bars table. Exiting."); sys.exit(1)
        target_date_str = latest_bar_response.data[0]['date']
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        print(f"  -> Target date for calculation is: {target_date_str}")
    except Exception as e:
        print(f"  -> Error determining target date: {e}. Exiting."); sys.exit(1)

    # ===> [THE MASTERSTROKE] Adopting your successful paginated data fetching strategy <===
    print("\n--- Step 2: Fetching all necessary historical data via pagination ---")
    all_historical_data = []
    page = 0
    start_date_fetch = (target_date - timedelta(days=STRATEGY_PERIOD + 60)).strftime('%Y-%m-%d')
    while True:
        print(f"  -> Fetching page {page+1}...")
        response = supabase.table('daily_bars') \
            .select('symbol, date, open, high, low, close, volume') \
            .gte('date', start_date_fetch) \
            .lte('date', target_date_str) \
            .order('date', desc=False) \
            .range(page * 20000, (page + 1) * 20000 - 1).execute() # 使用更大的分页以提高效率
        
        if not response.data: break
        all_historical_data.extend(response.data)
        # 假设如果返回的数据少于分页大小，就是最后一页
        if len(response.data) < 20000: break
        page += 1

    if not all_historical_data:
        print("--- No historical data found for the required period. Job finished. ---")
        return

    df = pd.DataFrame(all_historical_data)
    df['date'] = pd.to_datetime(df['date']).dt.date
    print(f"  -> Successfully fetched a total of {len(df)} rows.")

    # --- Step 3: Calculate all indicators in a single, robust operation ---
    def calculate_all_indicators(group):
        # 像成功代码一样，如果数据不足，返回原始组
        if len(group) < 30: return group 
        
        CLOSE = group['close']; HIGH = group['high']; LOW = group['low']; VOLUME = group['volume']
        
        # 计算所有短期指标
        group['ma10'] = MA(CLOSE, 10); group['ma20'] = MA(CLOSE, 20)
        group['ma50'] = MA(CLOSE, 50); group['ma60'] = MA(CLOSE, 60)
        group['volume_ma10'] = VOLUME.rolling(window=10).mean(); group['volume_ma30'] = VOLUME.rolling(window=30).mean()
        group['volume_ma60'] = VOLUME.rolling(window=60).mean(); group['volume_ma90'] = VOLUME.rolling(window=90).mean()
        DIF, DEA, _ = MACD(CLOSE.values); group['macd_diff'] = DIF; group['macd_dea'] = DEA
        group['rsi14'] = RSI(CLOSE.values, 14)
        
        # 只有在数据足够时才计算长周期指标
        if len(group) >= STRATEGY_PERIOD:
            group['ma150'] = MA(CLOSE, 150)
            group['high_52w'] = HIGH.rolling(window=STRATEGY_PERIOD, min_periods=1).max()
            group['low_52w'] = LOW.rolling(window=STRATEGY_PERIOD, min_periods=1).min()
            
            # 计算RS评分 (相对强度)
            start_price = group['close'].iloc[-STRATEGY_PERIOD]
            end_price = group['close'].iloc[-1]
            group['rs_raw'] = (end_price / start_price - 1) if start_price != 0 else 0
        
        return group

    print("\n--- Applying calculations to the entire dataset... ---")
    df_with_ta = df.groupby('symbol', group_keys=False).apply(calculate_all_indicators)
    print("  -> All calculations applied.")

    # --- Step 4: Filter, Rank, and Prepare for Upsert ---
    print("\n--- Filtering results for the target date... ---")
    today_indicators = df_with_ta[df_with_ta['date'] == target_date].copy()
    
    # 计算最终的RS评分 (0-100的百分位排名)
    if 'rs_raw' in today_indicators.columns:
        today_indicators['rs_rating'] = today_indicators['rs_raw'].rank(pct=True) * 100
        print("  -> RS Ratings calculated.")

    if today_indicators.empty:
        print("--- No indicators to update for the target date. Job finished. ---")
        return

    print(f"  -> Found {len(today_indicators)} stocks with data for the target date.")
    
    records_to_upsert = []
    indicator_columns = ['ma10', 'ma20', 'ma50', 'ma60', 'ma150', 'high_52w', 'low_52w', 'volume_ma10', 'volume_ma30', 'volume_ma60', 'volume_ma90', 'macd_diff', 'macd_dea', 'rsi14', 'rs_rating']
    for index, row in today_indicators.iterrows():
        record = {'symbol': row['symbol'], 'date': row['date'].strftime('%Y-%m-%d')}
        for col in indicator_columns:
            # 使用更安全的 .get() 方法
            if pd.notna(row.get(col)):
                record[col] = float(row.get(col))
        if len(record) > 2:
            records_to_upsert.append(record)

    # --- Step 5: Final Upsert ---
    if records_to_upsert:
        print(f"\n--- Upserting a total of {len(records_to_upsert)} calculated records ---")
        for i in range(0, len(records_to_upsert), BATCH_SIZE_UPSERT):
            upload_batch = records_to_upsert[i:i+BATCH_SIZE_UPSERT]
            print(f"    -> Upserting chunk {i//BATCH_SIZE_UPSERT + 1} ({len(upload_batch)} records)...")
            supabase.table('daily_metrics').upsert(upload_batch, on_conflict='symbol,date').execute()
        print("  -> All indicators updated successfully!")
    else:
        print("\n--- No new indicators were calculated to be upserted. ---")
        
    print("\n--- Job Finished ---")

if __name__ == '__main__':
    main()
