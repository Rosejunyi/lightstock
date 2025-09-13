# scripts/calculate_indicators.py
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

def main():
    print("--- Starting Job: Calculate Technical Indicators ---")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    target_date = datetime.now().date()
    target_date_str = target_date.strftime('%Y-%m-%d')
    
    print("Fetching recent historical data for calculation...")
    all_historical_data = []
    page = 0
    while True:
        response = supabase.table('daily_bars').select('symbol, date, open, high, low, close, volume').gte('date', (target_date - timedelta(days=150)).strftime('%Y-%m-%d')).lte('date', target_date_str).order('date', desc=False).range(page * 1000, (page + 1) * 1000 - 1).execute()
        if not response.data: break
        all_historical_data.extend(response.data)
        if len(response.data) < 1000: break
        page += 1
    
    if not all_historical_data:
        print("No historical data found. Job finished."); return

    df = pd.DataFrame(all_historical_data)
    print(f"Fetched {len(df)} rows for calculation.")
    
    def calculate_mytt(group):
        CLOSE = group['close'].values; HIGH = group['high'].values; LOW = group['low'].values; VOL = group['volume'].values.astype(float)
        if len(CLOSE) < 60: return group
        group['ma5'] = MA(CLOSE, 5); group['ma10'] = MA(CLOSE, 10); group['ma20'] = MA(CLOSE, 20); group['ma60'] = MA(CLOSE, 60)
        group['rsi14'] = RSI(CLOSE, 14)
        DIF, DEA, MACD_BAR = MACD(CLOSE); group['macd_diff'] = DIF; group['macd_dea'] = DEA
        K, D, J = KDJ(CLOSE, HIGH, LOW); group['kdj_k'] = K; group['kdj_d'] = D; group['kdj_j'] = J
        # ... (在这里添加更多 MyTT 指标计算) ...
        return group

    print("Calculating all indicators...")
    df_with_ta = df.groupby('symbol', group_keys=False).apply(calculate_mytt)
    
    today_indicators = df_with_ta[df_with_ta['date'] == target_date_str].copy()
    
    records_to_upsert = []
    indicator_columns = ['ma5', 'ma10', 'ma20', 'ma60', 'rsi14', 'macd_diff', 'macd_dea', 'kdj_k', 'kdj_d', 'kdj_j']
    
    for index, row in today_indicators.iterrows():
        record = {'symbol': row['symbol'], 'date': row['date']}
        for col in indicator_columns:
            if col in row and pd.notna(row[col]):
                record[col] = float(row[col])
        if len(record) > 2:
            records_to_upsert.append(record)
    
    if records_to_upsert:
        print(f"Upserting {len(records_to_upsert)} records with calculated indicators...")
        supabase.table('daily_metrics').upsert(records_to_upsert, on_conflict='symbol,date').execute()
        print("Technical indicators updated successfully!")
        
    print("--- Job Finished: Calculate Technical Indicators ---")

if __name__ == '__main__':
    main()
