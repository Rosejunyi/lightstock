# scripts/update_daily_bars.py (最终的、混合动力智能版)
    import os, sys, time
    from supabase import create_client, Client
    import baostock as bs
    import akshare as ak
    import pandas as pd
    from datetime import datetime, timedelta
    from dotenv import load_dotenv

    # ... (配置加载 & 辅助函数 get_... 和 get_... 保持不变) ...

    def main(supabase_url: str, supabase_key: str):
        print("--- Starting Job: [1/3] Smart Update Daily Bars (Hybrid Baostock + AKShare) ---")
        supabase: Client = create_client(supabase_url, supabase_key)
        
        # 1. 使用 AKShare 获取交易日历，进行智能判断
        print("Fetching trading calendar from AKShare...")
        try:
            trade_date_df = ak.tool_trade_date_hist_df()
            trade_dates = {pd.to_datetime(d).date() for d in trade_date_df['trade_date']}
        except Exception as e:
            print(f"  -> Could not fetch trading calendar: {e}. Exiting."); sys.exit(1)

        last_date_in_db = get_last_date_from_db(supabase)
        date_to_process = last_date_in_db + timedelta(days=1)
        today = datetime.now().date()
        
        # 筛选出需要处理的、真实的交易日
        dates_to_fetch = [d for d in sorted(list(trade_dates)) if date_to_process <= d <= today]
        
        if not dates_to_fetch:
            print("No new trading days to update. Job finished.")
            return

        print(f"Found {len(dates_to_fetch)} new trading day(s) to update: {', '.join([d.strftime('%Y-%m-%d') for d in dates_to_fetch])}")
        
        # 2. 登录 Baostock，准备获取数据
        lg = bs.login()
        if lg.error_code != '0':
            print(f"Baostock login failed: {lg.error_msg}"); sys.exit(1)
        print("Baostock login successful.")
        
        try:
            valid_symbols = get_valid_symbols_whitelist(supabase)
            print(f"Found {len(valid_symbols)} symbols to track.")
            total_upserted_count = 0

            # 3. 只遍历需要处理的真实交易日
            for trade_date in dates_to_fetch:
                date_str = trade_date.strftime('%Y-%m-%d')
                print(f"\n--- Processing date: {date_str} ---")
                
                records_for_today = []
                # ... (此处是 Baostock 逐只获取当天数据的 for 循环逻辑，完全不变) ...
                
                if records_for_today:
                    print(f"\n  -> Found {len(records_for_today)} valid records for {date_str}. Upserting now...")
                    # ... (分批上传逻辑) ...
                    total_upserted_count += len(records_for_today)
                    
        finally:
            bs.logout()
            print("\nBaostock logout successful.")
            print(f"\n--- Job Finished. Total records upserted: {total_upserted_count} ---")

    if __name__ == '__main__':
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("Error: Supabase credentials not found."); sys.exit(1)
        main(SUPABASE_URL, SUPABASE_KEY)
