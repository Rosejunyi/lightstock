# scripts/update_daily_metrics.py (最终的、完整的、Baostock 版)
import os
import sys
from supabase import create_client, Client
import baostock as bs
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# --- 配置 ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
# ---

def get_valid_symbols_whitelist(supabase_client: Client) -> set:
    """ 从 stocks_info 分页获取所有有效的股票 symbol 列表 """
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
    print("--- Starting Job: [2/3] Update Daily Metrics (Baostock Version) ---")
    supabase: Client = create_client(supabase_url, supabase_key)
    valid_symbols_whitelist = get_valid_symbols_whitelist(supabase)
    
    # 1. 登录 Baostock
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Baostock login failed: {lg.error_msg}"); sys.exit(1)
    print("Baostock login successful.")

    try:
        metrics_date = datetime.now().date()
        date_str_for_bs = metrics_date.strftime('%Y-%m-%d')
            
        print(f"Fetching daily metrics for all stocks for date: {date_str_for_bs}...")
        
        # --- 核心修复：使用正确的 Baostock 调用方式 ---
        # query_stock_basic 的 date 参数是可选的。我们传入日期，获取当天的快照。
        rs = bs.query_stock_basic(date=date_str_for_bs)
        
        if rs.error_code != '0':
            print(f"Failed to fetch stock basics from Baostock: {rs.error_msg}")
            # 如果带日期查询失败（比如当天是周末），就尝试获取最新的
            print("Retrying without date to get the latest available data...")
            rs = bs.query_stock_basic()
            if rs.error_code != '0':
                print(f"Retry also failed: {rs.error_msg}")
                return # 直接退出
        # ---------------------------------------------

        stock_basics_df = rs.get_data()
        print(f"Fetched {len(stock_basics_df)} total metric records from Baostock.")

        records_to_upsert = []
        
        for index, row in stock_basics_df.iterrows():
            # Baostock 的 code 格式是 'sh.600001'，我们需要转换
            code_parts = row['code'].split('.')
            if len(code_parts) != 2: continue
            symbol = f"{code_parts[1]}.{code_parts[0].upper()}" # 'sh.600001' -> '600001.SH'
            
            # 使用白名单过滤
            if symbol not in valid_symbols_whitelist:
                continue

            try:
                # 安全地转换数据
                pe = float(row['peTTM']) if row['peTTM'] and row['peTTM'] != '' else None
                pb = float(row['pbMRQ']) if row['pbMRQ'] and row['pbMRQ'] != '' else None
                total_market_cap = int(float(row['marketValue']) * 10000) if row['marketValue'] and row['marketValue'] != '' else None
                float_market_cap = int(float(row['flowValue']) * 10000) if row['flowValue'] and row['flowValue'] != '' else None
                turnover_rate = float(row['turnoverRatio']) if row['turnoverRatio'] and row['turnoverRatio'] != '' else None

                record = {
                    'symbol': symbol,
                    'date': date_str_for_bs, # 使用我们查询的日期
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

if __name__ == '__main__':
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found."); sys.exit(1)
    main(SUPABASE_URL, SUPABASE_KEY)
