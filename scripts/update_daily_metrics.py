# scripts/update_daily_metrics.py (最终的、完整的、缩进和逻辑修复版)
import os
import sys
from supabase import create_client, Client
import baostock as bs
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# --- 配置加载在顶层，必须顶格 ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# --- 函数定义，必须顶格 ---
def get_valid_symbols_whitelist(supabase_client: Client) -> set:
    # 函数内部的代码，必须缩进
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
    # 函数内部的代码，必须缩进
    print("--- Starting Job: [2/3] Update Daily Metrics (Baostock Version) ---")
        
    supabase: Client = create_client(supabase_url, supabase_key)
    valid_symbols_whitelist = get_valid_symbols_whitelist(supabase)
    
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Baostock login failed: {lg.error_msg}"); sys.exit(1)
    print("Baostock login successful.")

    try:
        metrics_date = datetime.now().date()
        date_str_for_db = metrics_date.strftime('%Y-%m-%d')
            
        print(f"Fetching latest daily metrics for all stocks...")
        
        # Baostock 的 query_stock_basic 默认返回最新的数据，不需要 date 参数
        rs = bs.query_stock_basic()
        if rs.error_code != '0':
            print(f"Failed to fetch stock basics from Baostock: {rs.error_msg}")
            return

        stock_basics_df = rs.get_data()
        print(f"Fetched {len(stock_basics_df)} total metric records from Baostock.")

        records_to_upsert = []
        
        for index, row in stock_basics_df.iterrows():
            code_parts = row['code'].split('.')
            if len(code_parts) != 2: continue
            symbol = f"{code_parts[1]}.{code_parts[0].upper()}"
            
            if symbol not in valid_symbols_whitelist:
                continue

            try:
                pe = float(row.get('peTTM')) if row.get('peTTM') and row.get('peTTM') != '' else None
                pb = float(row.get('pbMRQ')) if row.get('pbMRQ') and row.get('pbMRQ') != '' else None
                total_market_cap = int(float(row.get('marketValue', 0)) * 10000) if row.get('marketValue') and row.get('marketValue') != '' else None
                float_market_cap = int(float(row.get('flowValue', 0)) * 10000) if row.get('flowValue') and row.get('flowValue') != '' else None
                turnover_rate = float(row.get('turnoverRatio')) if row.get('turnoverRatio') and row.get('turnoverRatio') != '' else None

                record = {
                    'symbol': symbol,
                    'date': date_str_for_db,
                    'pe_ratio_dynamic': pe,
                    'pb_ratio': pb,
                    'total_market_cap': total_market_cap,
                    'float_market_cap': float_market_cap,
                    'turnover_rate': turnover_rate
                }
                records_to_upsert.append(record)
            except (ValueError, TypeError):
                continue
        
        if records_to_upsert:
            print(f"Upserting {len(records_to_upsert)} valid metric records to daily_metrics...")
            batch_size = 500
            for i in range(0, len(records_to_upsert), batch_size):
                batch = records_to_upsert[i:i+batch_size]
                supabase.table('daily_metrics').upsert(batch, on_conflict='symbol,date').execute()
            print("daily_metrics table updated successfully!")
            
    finally:
        bs.logout()
        print("\nBaostock logout successful.")
        print("--- Job Finished: Update Daily Metrics ---")

# --- 程序的启动入口，必须顶格 ---
if __name__ == '__main__':
    # 内部的代码，必须缩进
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found in environment or .env file.")
        sys.exit(1)
    main(SUPABASE_URL, SUPABASE_KEY)
