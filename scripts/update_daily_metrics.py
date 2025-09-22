# scripts/update_daily_metrics.py
import os, sys
from supabase import create_client, Client
import baostock as bs
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_valid_symbols_whitelist(supabase_client: Client) -> set:
    all_symbols = set()
    page = 0
    while True:
        response = supabase_client.table('stocks_info').select('symbol').range(page * 1000, (page + 1) * 1000 - 1).execute()
        if not response.data: break
        all_symbols.update(item['symbol'] for item in response.data)
        if len(response.data) < 1000: break
        page += 1
    return all_symbols
    
def main(supabase_url: str, supabase_key: str):
    print("--- Starting Job: [2/3] Update Daily Metrics (Baostock Version) ---")
    supabase: Client = create_client(supabase_url, supabase_key)
    valid_symbols_whitelist = get_valid_symbols_whitelist(supabase)
    
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Baostock login failed: {lg.error_msg}"); sys.exit(1)
    print("Baostock login successful.")

    try:
        date_str = datetime.now().date().strftime('%Y-%m-%d')
        bs_codes_str = ",".join([f"{s.split('.')[1].lower()}.{s.split('.')[0]}" for s in valid_symbols_whitelist])
            
        print(f"Fetching daily metrics for {len(valid_symbols_whitelist)} stocks for date: {date_str}...")
        
        # Baostock 的估值指标，参数是 code 和 date
        rs = bs.query_stock_basic(code_name=bs_codes_str, date=date_str)
        if rs.error_code != '0':
            print(f"Failed to fetch stock basics: {rs.error_msg}"); return

        stock_basics_df = rs.get_data()
        print(f"Fetched {len(stock_basics_df)} metric records from Baostock.")

        records_to_upsert = []
        symbol_map = {f"sh.{row['code']}": f"{row['code']}.SH" for index, row in stock_basics_df.iterrows()}
        symbol_map.update({f"sz.{row['code']}": f"{row['code']}.SZ" for index, row in stock_basics_df.iterrows()})

        for index, row in stock_basics_df.iterrows():
            try:
                symbol = symbol_map.get(row['code'])
                if not symbol: continue
                
                record = {
                    'symbol': symbol, 'date': date_str,
                    'pe_ratio_dynamic': float(row['peTTM']) if row['peTTM'] else None,
                    'pb_ratio': float(row['pbMRQ']) if row['pbMRQ'] else None,
                    'total_market_cap': int(float(row['marketValue']) * 10000) if row['marketValue'] else None,
                    'float_market_cap': int(float(row['flowValue']) * 10000) if row['flowValue'] else None,
                    'turnover_rate': float(row['turnoverRatio']) if row['turnoverRatio'] else None
                }
                records_to_upsert.append(record)
            except: continue
        
        if records_to_upsert:
            print(f"Upserting {len(records_to_upsert)} valid metric records to daily_metrics...")
            supabase.table('daily_metrics').upsert(records_to_upsert, on_conflict='symbol,date').execute()
            print("daily_metrics table updated successfully!")
            
    finally:
        bs.logout()
        print("\nBaostock logout successful.")
        print("--- Job Finished: Update Daily Metrics ---")

if __name__ == '__main__':
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found."); sys.exit(1)
    main(SUPABASE_URL, SUPABASE_KEY)
