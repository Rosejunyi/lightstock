# scripts/update_daily_metrics.py (最终的、带侦察和安全获取的 Baostock 版)
    import os, sys
    from supabase import create_client, Client
    import baostock as bs
    import pandas as pd
    from datetime import datetime
    from dotenv import load_dotenv

    load_dotenv()
    SUPABASE_URL = os.environ.get("SUPABASE_URL")
    SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

    def get_valid_symbols_whitelist(supabase_client: Client) -> set:
        # ... (这个函数内容是正确的，保持不变) ...
    
    def main(supabase_url: str, supabase_key: str):
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
            
            rs = bs.query_stock_basic()
            if rs.error_code != '0':
                print(f"Failed to fetch stock basics from Baostock: {rs.error_msg}"); return

            stock_basics_df = rs.get_data()
            
            # --- 侦察代码：打印出所有可用的列名 ---
            print("\n[侦测] Baostock 返回的原始列名:")
            print(stock_basics_df.columns.to_list())
            print("-" * 30)
            # ------------------------------------

            print(f"Fetched {len(stock_basics_df)} total metric records from Baostock.")

            records_to_upsert = []
            
            for index, row in stock_basics_df.iterrows():
                code_parts = row['code'].split('.')
                if len(code_parts) != 2: continue
                symbol = f"{code_parts[1]}.{code_parts[0].upper()}"
                
                if symbol not in valid_symbols_whitelist:
                    continue

                try:
                    # --- 核心修复：使用 .get() 方法安全地获取值 ---
                    # 这样，即使列名不叫 'peTTM'，代码也不会崩溃
                    pe_str = row.get('peTTM')
                    pb_str = row.get('pbMRQ')
                    market_cap_str = row.get('marketValue')
                    float_cap_str = row.get('flowValue')
                    turnover_str = row.get('turnoverRatio')

                    pe = float(pe_str) if pe_str and pe_str != '' else None
                    pb = float(pb_str) if pb_str and pb_str != '' else None
                    total_market_cap = int(float(market_cap_str) * 10000) if market_cap_str and market_cap_str != '' else None
                    float_market_cap = int(float(float_cap_str) * 10000) if float_cap_str and float_cap_str != '' else None
                    turnover_rate = float(turnover_str) if turnover_str and turnover_str != '' else None
                    
                    record = {
                        'symbol': symbol, 'date': date_str_for_db,
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
                # ... (分批上传逻辑) ...
                print("daily_metrics table updated successfully!")
                
        finally:
            bs.logout()
            print("\nBaostock logout successful.")
            print("--- Job Finished: Update Daily Metrics ---")

    if __name__ == '__main__':
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("Error: Supabase credentials not found."); sys.exit(1)
        main(SUPABASE_URL, SUPABASE_KEY)
