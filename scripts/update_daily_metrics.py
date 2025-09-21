# scripts/update_daily_metrics.py
import os, sys
from supabase import create_client, Client
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_valid_symbols_whitelist(supabase_client: Client) -> set:
    print("Fetching whitelist from stocks_info...")
    all_symbols = set()
    page = 0
    while True:
        response = supabase_client.table('stocks_info').select('symbol').range(page * 1000, (page + 1) * 1000 - 1).execute()
        if not response.data: break
        all_symbols.update(item['symbol'] for item in response.data)
        if len(response.data) < 1000: break
        page += 1
    print(f"  -> Whitelist created with {len(all_symbols)} symbols.")
    return all_symbols
    
def main(supabase_url: str, supabase_key: str):
    print("--- Starting Job: [2/3] Update Daily Metrics ---")
        
    supabase: Client = create_client(supabase_url, supabase_key)
    valid_symbols_whitelist = get_valid_symbols_whitelist(supabase)
    
    print("Fetching real-time metrics from AKShare...")
    metrics_df = ak.stock_zh_a_spot_em()
    if metrics_df is None or metrics_df.empty:
        print("Could not fetch daily metrics. Job finished."); return

    print(f"Fetched {len(metrics_df)} metric records.")
    
    metrics_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df_cleaned = metrics_df.astype(object).where(pd.notna(metrics_df), None)
    
    records_to_upsert = []
    metrics_date = datetime.now().date().strftime('%Y-%m-%d')
    
    for row in df_cleaned.to_dict('records'):
        code = str(row.get('代码'))
        if not code: continue
        market = 'SH' if code.startswith(('60','68')) else 'SZ'
        symbol = f"{code}.{market}"
        
        if symbol not in valid_symbols_whitelist:
            continue
        
        record = {
            'symbol': symbol, 'date': metrics_date,
            'pe_ratio_dynamic': row.get('市盈率-动态'),
            'pb_ratio': row.get('市净率'),
            'total_market_cap': row.get('总市值'),
            'float_market_cap': row.get('流通市值'),
            'turnover_rate': row.get('换手率')
        }
        try:
            if record['total_market_cap'] is not None: record['total_market_cap'] = int(record['total_market_cap'])
            if record['float_market_cap'] is not None: record['float_market_cap'] = int(record['float_market_cap'])
        except (ValueError, TypeError): continue
        records_to_upsert.append(record)

    if records_to_upsert:
        print(f"Upserting {len(records_to_upsert)} valid metric records to daily_metrics...")
        batch_size = 500
        for i in range(0, len(records_to_upsert), batch_size):
            batch = records_to_upsert[i:i+batch_size]
            supabase.table('daily_metrics').upsert(batch, on_conflict='symbol,date').execute()

        print("daily_metrics table updated successfully!")
        
    print("--- Job Finished: Update Daily Metrics ---")

if __name__ == '__main__':
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found."); sys.exit(1)
    main(SUPABASE_URL, SUPABASE_KEY)
