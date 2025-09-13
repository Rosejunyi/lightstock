# scripts/calculate_indicators.py (The Grandmaster Edition - Built on the proven success pattern)
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
BATCH_SIZE = 100
print(f"\n!!! RUNNING GRANDMASTER EDITION (Batch Size: {BATCH_SIZE}, Period: {STRATEGY_PERIOD}) !!!\n")

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

    # --- Step 2: Prepare Symbol List ---
    try:
        response = supabase.table('stocks_info').select('symbol').execute()
        all_symbols = [item['symbol'] for item in response.data]
        print(f"  -> Found {len(all_symbols)} symbols for calculation.")
    except Exception as e:
        print(f"  -> Error fetching symbol list: {e}. Exiting."); sys.exit(1)

    # --- Step 3: Process All Symbols in Batches ---
    all_records_to_upsert = []
    total_batches = (len(all_symbols) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(all_symbols), BATCH_SIZE):
        batch_symbols = all_symbols[i:i+BATCH_SIZE]
        current_batch_num = i//BATCH_SIZE + 1
        print(f"\n--- Processing batch {current_batch_num}/{total_batches} ---")
        
        try:
            # 1. Fetch data for the current batch
            start_date_batch = (target_date - timedelta(days=STRATEGY_PERIOD + 60)).strftime('%Y-%m-%d')
            response = supabase.table('daily_bars') \
                .select('symbol, date, open, high, low, close, volume') \
                .in_('symbol', batch_symbols) \
                .gte('date', start_date_batch) \
                .lte('date', target_date_str) \
                .order('date', desc=False) \
                .execute()
            
            if not response.data:
                print("  -> No historical data found for this batch. Skipping.")
                continue
            
            df_batch = pd.DataFrame(response.data)
            df_batch['date'] = pd.to_datetime(df_batch['date']).dt.date
            
            # 2. Define the robust calculation function
            def calculate_all_indicators(group):
                if len(group) < 30: return None # Safety check: need at least 30 days
                
                CLOSE = group['close']; HIGH = group['high']; LOW = group['low']; VOLUME = group['volume']
                
                # Always calculate short-term indicators
                group['ma10'] = MA(CLOSE, 10); group['ma20'] = MA(CLOSE, 20)
                group['ma50'] = MA(CLOSE, 50); group['ma60'] = MA(CLOSE, 60)
                group['volume_ma10'] = VOLUME.rolling(window=10).mean()
                group['volume_ma30'] = VOLUME.rolling(window=30).mean()
                group['volume_ma60'] = VOLUME.rolling(window=60).mean()
                group['volume_ma90'] = VOLUME.rolling(window=90).mean()
                DIF, DEA, _ = MACD(CLOSE.values); group['macd_diff'] = DIF; group['macd_dea'] = DEA
                group['rsi14'] = RSI(CLOSE.values, 14)
                
                # Only calculate long-term indicators if data is sufficient
                if len(group) >= STRATEGY_PERIOD:
                    group['ma150'] = MA(CLOSE, 150)
                    group['high_52w'] = HIGH.rolling(window=STRATEGY_PERIOD, min_periods=1).max()
                    group['low_52w'] = LOW.rolling(window=STRATEGY_PERIOD, min_periods=1).min()
                
                return group

            # 3. Apply the calculation
            df_with_ta = df_batch.groupby('symbol', group_keys=False).apply(calculate_all_indicators)
            
            # ===> [THE GRANDMASTER FIX] This is the correct, robust way to handle failed calculations <===
            if df_with_ta.empty:
                print("  -> No stocks in this batch had enough data to pass the initial check. Skipping.")
                continue

            # 4. Filter for the target date
            today_indicators = df_with_ta[df_with_ta['date'] == target_date].copy()

            if today_indicators.empty:
                print(f"  -> Data for target date {target_date_str} not found in this batch's results. Skipping.")
                continue
            
            # 5. Prepare records for this batch
            indicator_columns = ['ma10', 'ma20', 'ma50', 'ma60', 'ma150', 'high_52w', 'low_52w', 'volume_ma10', 'volume_ma30', 'volume_ma60', 'volume_ma90', 'macd_diff', 'macd_dea', 'rsi14']
            for index, row in today_indicators.iterrows():
                record = {'symbol': row['symbol'], 'date': row['date'].strftime('%Y-%m-%d')}
                for col in indicator_columns:
                    if col in row and pd.notna(row[col]):
                        record[col] = float(row[col])
                if len(record) > 2:
                    all_records_to_upsert.append(record)

        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"  -> An unhandled error occurred in batch {current_batch_num}. Skipping.")
    
    # --- Step 4: Final Upsert ---
    if all_records_to_upsert:
        print(f"\n--- Upserting a total of {len(all_records_to_upsert)} calculated records ---")
        for i in range(0, len(all_records_to_upsert), BATCH_SIZE):
            upload_batch = all_records_to_upsert[i:i+BATCH_SIZE]
            print(f"    -> Upserting chunk {i//BATCH_SIZE + 1} ({len(upload_batch)} records)...")
            supabase.table('daily_metrics').upsert(upload_batch, on_conflict='symbol,date').execute()
        print("  -> All indicators updated successfully!")
    else:
        print("\n--- No new indicators were calculated to be upserted. ---")
        
    print("\n--- Job Finished ---")

if __name__ == '__main__':
    main()
