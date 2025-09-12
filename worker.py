# worker.py (GitHub Actions - 最终的 Daily Bars + Daily Metrics 同步版)
import os
import sys
from supabase import create_client, Client
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# ... (get_last_trade_date_from_db 函数保持不变) ...
def get_last_trade_date_from_db(supabase_client):
    try:
        response = supabase_client.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
        if response.data:
            return datetime.strptime(response.data[0]['date'], '%Y-%m-%d').date()
    except Exception as e:
        print(f"Warning: Could not get last trade date: {e}")
    return datetime.strptime("2025-01-01", "%Y-%m-%d").date()


def do_update_job():
    print("--- Starting Daily Full Data Update Job ---")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        # 1. 获取日线数据 (daily_bars)
        print("\n--- Step 1: Updating Daily Bars ---")
        last_date_in_db = get_last_trade_date_from_db(supabase)
        current_date_to_process = last_date_in_db + timedelta(days=1)
        today = datetime.now().date()

        if current_date_to_process > today:
            print("Daily bars are already up to date.")
        else:
            # ... (这里是之前那个完整的、逐天回填 daily_bars 的逻辑) ...
            # ... (为了简洁，我暂时省略，请确保你使用的是那个完整的版本) ...
            print("Daily bars update finished.")


        # 2. 获取并更新每日指标 (daily_metrics)
        print("\n--- Step 2: Updating Daily Metrics ---")
        print("Fetching real-time metrics from AKShare (stock_zh_a_spot_em)...")
        metrics_df = ak.stock_zh_a_spot_em()
        if metrics_df is None or metrics_df.empty:
            print("Could not fetch daily metrics. Skipping this step.")
        else:
            print(f"Fetched {len(metrics_df)} metric records.")
            
            records_to_upsert = []
            metrics_date = today.strftime('%Y-%m-%d')
            for index, row in metrics_df.iterrows():
                code = str(row['代码'])
                market = 'SH' if code.startswith(('60','68')) else 'SZ'
                symbol = f"{code}.{market}"
                
                records_to_upsert.append({
                    'symbol': symbol,
                    'date': metrics_date,
                    'pe_ratio_dynamic': row.get('市盈率-动态'),
                    'pb_ratio': row.get('市净率'),
                    'total_market_cap': row.get('总市值'),
                    'float_market_cap': row.get('流通市值'),
                    'turnover_rate': row.get('换手率')
                })

            if records_to_upsert:
                print(f"Upserting {len(records_to_upsert)} records to daily_metrics...")
                supabase.table('daily_metrics').upsert(records_to_upsert, on_conflict='symbol,date').execute()
                print("daily_metrics table updated successfully!")

    except Exception as e:
        print(f"An unhandled error occurred: {e}"); sys.exit(1)
    finally:
        print("\n--- Daily Full Data Update Job FINISHED ---")

if __name__ == '__main__':
    do_update_job()
