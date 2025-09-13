# scripts/calculate_indicators.py (全功率最终版 - 已修复所有缩进问题)
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

def main():
    print("--- Starting Job: [3/3] Calculate All Indicators (Full Power Engine) ---")
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found."); sys.exit(1)
        
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print("\n--- Step 1: Determining target calculation date ---")
    try:
        latest_bar_response = supabase.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
        if not latest_bar_response.data:
            print("Error: No data in daily_bars table. Exiting."); sys.exit(1)
        target_date = datetime.strptime(latest_bar_response.data[0]['date'], '%Y-%m-%d').date()
        target_date_str = target_date.strftime('%Y-%m-%d')
        print(f"  -> Target date for calculation is: {target_date_str}")
    except Exception as e:
        print(f"  -> Error fetching latest date: {e}. Exiting."); sys.exit(1)

    print("\n--- Step 2: Calculating all indicators in Python ---")
    try:
        print("  Fetching whitelist from stocks_info...")
        response = supabase.table('stocks_info').select('symbol').execute()
        all_symbols = [item['symbol'] for item in response.data]
        print(f"  -> Found {len(all_symbols)} symbols for calculation.")

        print("\n  --- Calculating RS Ratings for all stocks ---")
        start_date_rs = (target_date - timedelta(days=400)).strftime('%Y-%m-%d')
        response = supabase.table('daily_bars') \
            .select('symbol, date, close') \
            .gte('date', start_date_rs) \
            .lte('date', target_date_str) \
            .in_('symbol', all_symbols) \
            .order('date', desc=False) \
            .execute()
        
        df_all = pd.DataFrame(response.data)
        
        def calculate_return(group):
            if len(group) < 250: return None
            start_price = group['close'].iloc[-250]
            end_price = group['close'].iloc[-1]
            return (end_price / start_price - 1) if start_price != 0 else 0

        returns = df_all.groupby('symbol').apply(calculate_return).dropna()
        rs_ratings = returns.rank(pct=True) * 100

        # ===> [已修复] 兼容性及缩进问题 <===
        rs_ratings_df = rs_ratings.reset_index()
        rs_ratings_df = rs_ratings_df.rename(columns={0: 'rs_rating', 'symbol': 'symbol'})
        
        rs_ratings_df['date'] = target_date_str
        print(f"  -> RS Ratings calculated for {len(rs_ratings_df)} stocks.")
        
        if not rs_ratings_df.empty:
            supabase.table('daily_metrics').upsert(rs_ratings_df.to_dict('records'), on_conflict='symbol,date').execute()
            print("  -> RS Ratings upserted successfully.")

        batch_size = 150 
        total_batches = (len(all_symbols) + batch_size - 1) // batch_size

        for i in range(0, len(all_symbols), batch_size):
            batch_symbols = all_symbols[i:i+batch_size]
            current_batch_num = i//batch_size + 1
            print(f"\n  --- Processing indicator batch {current_batch_num}/{total_batches} ---")
            
            start_date_batch = (target_date - timedelta(days=400)).strftime('%Y-%m-%d')
            response = supabase.table('daily_bars') \
                .select('symbol, date, open, high, low, close, volume') \
                .in_('symbol', batch_symbols) \
                .gte('date', start_date_batch) \
                .lte('date', target_date_str) \
                .order('date', desc=False) \
                .execute()
            
            if not response.data:
                print("    -> No historical data for this batch. Skipping.")
                continue
            
            df_batch = pd.DataFrame(response.data)
            
            def calculate_all_indicators(group):
                CLOSE = group['close']; HIGH = group['high']; LOW = group['low']; VOLUME = group['volume']
                group['ma10'] = MA(CLOSE, 10); group['ma20'] = MA(CLOSE, 20); group['ma50'] = MA(CLOSE, 50)
                group['ma150'] = MA(CLOSE, 150); group['ma200'] = MA(CLOSE, 200)
                group['high_52w'] = HIGH.rolling(window=250, min_periods=1).max()
                group['low_52w'] = LOW.rolling(window=250, min_periods=1).min()
                group['volume_ma10'] = VOLUME.rolling(window=10).mean()
                group['volume_ma30'] = VOLUME.rolling(window=30).mean()
                group['volume_ma60'] = VOLUME.rolling(window=60).mean()
                group['volume_ma90'] = VOLUME.rolling(window=90).mean()
                if len(CLOSE) >= 30:
                    DIF, DEA, _ = MACD(CLOSE.values); group['macd_diff'] = DIF; group['macd_dea'] = DEA
                    group['rsi14'] = RSI(CLOSE.values, 14)
                return group

            df_with_ta = df_batch.groupby('symbol', group_keys=False).apply(calculate_all_indicators)
            today_indicators = df_with_ta[df_with_ta['date'] == target_date_str].copy()
            
            records_to_upsert = []
            indicator_columns = [
                'ma10', 'ma20', 'ma50', 'ma150', 'ma200', 'high_52w', 'low_52w',
                'volume_ma10', 'volume_ma30', 'volume_ma60', 'volume_ma90',
                'macd_diff', 'macd_dea', 'rsi14'
            ]
            for index, row in today_indicators.iterrows():
                record = {'symbol': row['symbol'], 'date': row['date']}
                for col in indicator_columns:
                    if col in row and pd.notna(row[col]):
                        record[col] = float(row[col])
                if len(record) > 2:
                    records_to_upsert.append(record)
            
            if records_to_upsert:
                print(f"    -> Calculated indicators for {len(records_to_upsert)} stocks. Upserting...")
                supabase.table('daily_metrics').upsert(records_to_upsert, on_conflict='symbol,date').execute()

    except Exception as e:
        print(f"  -> An error occurred during Python calculation: {e}")
        # 增加这行来打印更详细的错误信息
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    print("\n--- Job Finished: Calculate All Indicators ---")

if __name__ == '__main__':
    main()
