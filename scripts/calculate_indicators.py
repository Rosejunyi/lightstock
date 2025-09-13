# scripts/calculate_indicators.py (终极稳健版)
import os, sys, pandas as pd, numpy as np
from supabase import create_client, Client
from datetime import datetime, timedelta
from dotenv import load_dotenv
from MyTT import *

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

STRATEGY_PERIOD = 120 
PAGINATION_SIZE = 1000 
UPSERT_BATCH_SIZE = 200
print(f"\n!!! RUNNING ULTIMATE ROBUST EDITION (Pagination: {PAGINATION_SIZE}) !!!\n")

def main():
    print("--- Starting Job: [3/3] Calculate All Indicators ---")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    try:
        latest_bar_response = supabase.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
        target_date_str = latest_bar_response.data[0]['date']
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        print(f"  -> Target date for calculation is: {target_date_str}")
    except Exception as e:
        print(f"  -> Error determining target date: {e}. Exiting."); sys.exit(1)

    all_historical_data = []
    page = 0
    start_date_fetch = (target_date - timedelta(days=STRATEGY_PERIOD + 90)).strftime('%Y-%m-%d')
    while True:
        print(f"  -> Fetching page {page+1}...")
        response = supabase.table('daily_bars').select('symbol, date, open, high, low, close, volume').gte('date', start_date_fetch).lte('date', target_date_str).order('date', desc=False).range(page * PAGINATION_SIZE, (page + 1) * PAGINATION_SIZE - 1).execute()
        if not response.data: break
        all_historical_data.extend(response.data)
        if len(response.data) < PAGINATION_SIZE: break
        page += 1

    if not all_historical_data:
        print("--- No historical data found. Job finished. ---"); return

    df = pd.DataFrame(all_historical_data)
    df['date'] = pd.to_datetime(df['date']).dt.date
    print(f"  -> Successfully fetched a total of {len(df)} rows.")

    def calculate_all_indicators(group):
        cols_to_create = ['ma10','ma20','ma50','ma60','ma150','high_52w','low_52w','volume_ma10','volume_ma30','volume_ma60','volume_ma90','macd_diff','macd_dea','rsi14','rs_raw']
        for col in cols_to_create: group[col] = np.nan
        if len(group) < 30: return group
        CLOSE = group['close']; HIGH = group['high']; LOW = group['low']; VOLUME = group['volume']
        group['ma10'] = MA(CLOSE, 10); group['ma20'] = MA(CLOSE, 20); group['ma50'] = MA(CLOSE, 50); group['ma60'] = MA(CLOSE, 60)
        group['volume_ma10'] = VOLUME.rolling(window=10).mean(); group['volume_ma30'] = VOLUME.rolling(window=30).mean()
        group['volume_ma60'] = VOLUME.rolling(window=60).mean(); group['volume_ma90'] = VOLUME.rolling(window=90).mean()
        DIF, DEA, _ = MACD(CLOSE.values); group['macd_diff'] = DIF; group['macd_dea'] = DEA
        group['rsi14'] = RSI(CLOSE.values, 14)
        if len(group) >= STRATEGY_PERIOD:
            group['ma150'] = MA(CLOSE, 150)
            group['high_52w'] = HIGH.rolling(window=STRATEGY_PERIOD, min_periods=1).max()
            group['low_52w'] = LOW.rolling(window=STRATEGY_PERIOD, min_periods=1).min()
            start_price = group['close'].iloc[-STRATEGY_PERIOD]; end_price = group['close'].iloc[-1]
            group['rs_raw'] = (end_price / start_price - 1) if start_price != 0 else 0
        return group

    print("--- Applying calculations...")
    df_with_ta = df.groupby('symbol', group_keys=False).apply(calculate_all_indicators)
    print("--- Filtering for target date...")
    today_indicators = df_with_ta[df_with_ta['date'] == target_date].copy()
    
    if 'rs_raw' in today_indicators.columns:
        today_indicators['rs_rating'] = today_indicators['rs_raw'].rank(pct=True) * 100
    
    records_to_upsert = []
    final_cols = ['ma10','ma20','ma50','ma60','ma150','high_52w','low_52w','volume_ma10','volume_ma30','volume_ma60','volume_ma90','macd_diff','macd_dea','rsi14','rs_rating']
    for index, row in today_indicators.iterrows():
        record = {'symbol': row['symbol'], 'date': row['date'].strftime('%Y-%m-%d')}
        for col in final_cols:
            if pd.notna(row.get(col)): record[col] = float(row.get(col))
        # 即使只有一个指标算出来，我们也上传
        if len(record) > 2: records_to_upsert.append(record)

    if records_to_upsert:
        print(f"--- Upserting {len(records_to_upsert)} records ---")
        for i in range(0, len(records_to_upsert), UPSERT_BATCH_SIZE):
            upload_batch = records_to_upsert[i:i+UPSERT_BATCH_SIZE]
            print(f"    -> Upserting chunk {i//UPSERT_BATCH_SIZE + 1}...")
            supabase.table('daily_metrics').upsert(upload_batch, on_conflict='symbol,date').execute()
        print("  -> All indicators updated successfully!")
    else:
        print("--- No new indicators were calculated. ---")
    print("--- Job Finished ---")

if __name__ == '__main__':
    main()
