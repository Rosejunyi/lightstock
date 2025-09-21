# scripts/update_daily_bars.py (最终的、逐日原子更新版)
import os, sys, time
from supabase import create_client, Client
import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# ... (配置和辅助函数 get_... 和 get_... 保持不变) ...

def main():
    print("--- Starting Job: [1/3] Update Daily Bars (Atomic Daily Update) ---")
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found."); sys.exit(1)
        
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Baostock login failed: {lg.error_msg}"); sys.exit(1)
    print("Baostock login successful.")

    total_upserted_count = 0
    try:
        last_date_in_db = get_last_date_from_db(supabase)
        date_to_process = last_date_in_db + timedelta(days=1)
        today = datetime.now().date()

        if date_to_process > today:
            print("Daily bars are already up to date. Job finished."); return
            
        print(f"Starting backfill from {date_to_process} up to {today}.")
        valid_symbols = get_valid_symbols_whitelist(supabase)
        print(f"Found {len(valid_symbols)} symbols to track.")
        
        # --- 核心修复：逐日处理和上传 ---
        while date_to_process <= today:
            date_str = date_to_process.strftime('%Y-%m-%d')
            print(f"\n--- Processing date: {date_str} ---")
            
            # 为当天的数据准备一个独立的列表
            records_for_today = []
            
            # 逐只股票获取数据
            for i, symbol in enumerate(list(valid_symbols)):
                sys.stdout.write(f"\r  -> Fetching {i+1}/{len(valid_symbols)}: {symbol}...")
                sys.stdout.flush()

                bs_code = f"{symbol.split('.')[1].lower()}.{symbol.split('.')[0]}"
                
                rs = bs.query_history_k_data_plus(bs_code,
                    "date,code,open,high,low,close,volume,amount",
                    start_date=date_str, end_date=date_str,
                    frequency="d", adjustflag="3")
                
                if rs.error_code == '0' and rs.next():
                    record = rs.get_row_data()
                    try:
                        if all(field != '' for field in record[2:8]):
                            records_for_today.append({
                                "symbol": symbol, "date": record[0],
                                "open": float(record[2]), "high": float(record[3]),
                                "low": float(record[4]), "close": float(record[5]),
                                "volume": int(record[6]), "amount": int(float(record[7]))
                            })
                    except (ValueError, TypeError): continue
                
                # 我们不再在这里分小批次上传，而是在处理完一整天后上传
                # time.sleep(0.01)

            # 在处理完一整天的所有股票后，统一上传当天的数据
            if records_for_today:
                print(f"\n  -> Found {len(records_for_today)} valid records for {date_str}. Upserting now...")
                # 分批上传当天的数据
                batch_size = 500
                for i in range(0, len(records_for_today), batch_size):
                    batch = records_for_today[i:i+batch_size]
                    supabase.table('daily_bars').upsert(batch).execute()

                total_upserted_count += len(records_for_today)
                print(f"  -> Successfully upserted data for {date_str}.")
            else:
                print(f"  -> No trading data found for {date_str} (likely not a trading day).")

            # 日期前进一天，准备处理下一天
            date_to_process += timedelta(days=1)
            
    finally:
        bs.logout()
        print("\nBaostock logout successful.")
        print(f"\n--- Job Finished: Update Daily Bars. Total records upserted in this run: {total_upserted_count} ---")

if __name__ == '__main__':
    main()
