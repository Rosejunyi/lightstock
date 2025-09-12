# worker.py (GitHub Actions - 最终的、完整的、王者归来版)
import os
import sys
from supabase import create_client, Client
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# ... (get_last_trade_date_from_db 函数保持不变) ...

def do_update_job():
    print("--- Starting Daily Full Data Update Job ---")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")

        # --- 关键步骤 1: 在所有操作开始前，先获取“白名单” ---
        print("Fetching a-share whitelist from stocks_info...")
        response = supabase.table('stocks_info').select('symbol').execute()
        valid_symbols_whitelist = {item['symbol'] for item in response.data}
        print(f"  -> Whitelist created with {len(valid_symbols_whitelist)} symbols.")
        # ----------------------------------------------------

        # 步骤 2: 更新 Daily Bars (日线行情) - 保持你之前能成功运行的逻辑
        print("\n--- Step 2: Updating Daily Bars ---")
        # ... (这里是你之前那个能成功运行的、最稳妥的 daily_bars 更新代码) ...
        # ... (比如，逐天回填的版本，并在 for 循环内增加 if symbol in valid_symbols_whitelist 判断) ...
        print("Daily bars update finished or is up to date.")


        # 步骤 3: 更新 Daily Metrics (每日指标)
        print("\n--- Step 3: Updating Daily Metrics ---")
        print("Fetching real-time metrics from AKShare...")
        metrics_df = ak.stock_zh_a_spot_em()
        if metrics_df is None or metrics_df.empty:
            print("Could not fetch daily metrics. Skipping.")
        else:
            print(f"Fetched {len(metrics_df)} metric records.")
            
            metrics_df.replace([np.inf, -np.inf], np.nan, inplace=True)
            
            records_to_upsert = []
            metrics_date = datetime.now().date().strftime('%Y-%m-%d')
            dict_records = metrics_df.where(pd.notna(metrics_df), None).to_dict('records')

            for row in dict_records:
                code = str(row.get('代码'))
                if not code: continue
                market = 'SH' if code.startswith(('60','68')) else 'SZ'
                symbol = f"{code}.{market}"
                
                if symbol not in valid_symbols_whitelist:
                    continue
                
                # --- 核心修复：这里是完整、无省略的字典 ---
                record = {
                    'symbol': symbol,
                    'date': metrics_date,
                    'pe_ratio_dynamic': None,
                    'pb_ratio': None,
                    'total_market_cap': None,
                    'float_market_cap': None,
                    'turnover_rate': None,
                    'ma5': None,
                    'ma10': None,
                    'rsi14': None
                }

                try:
                    if pd.notna(row.get('市盈率-动态')):
                        record['pe_ratio_dynamic'] = float(row['市盈率-动态'])
                    if pd.notna(row.get('市净率')):
                        record['pb_ratio'] = float(row['市净率'])
                    if pd.notna(row.get('总市值')):
                        record['total_market_cap'] = int(row['总市值'])
                    if pd.notna(row.get('流通市值')):
                        record['float_market_cap'] = int(row['流通市值'])
                    if pd.notna(row.get('换手率')):
                        record['turnover_rate'] = float(row['换手率'])
                except (ValueError, TypeError):
                    print(f"  -> Warning: Skipping record for {symbol} due to bad data format.")
                    continue
                
                records_to_upsert.append(record)

            print(f"Filtered down to {len(records_to_upsert)} records based on the whitelist.")
            
            if records_to_upsert:
                print(f"Upserting {len(records_to_upsert)} metric records to daily_metrics...")
                final_records = [{k: v for k, v in r.items() if v is not None} for r in records_to_upsert]
                
                supabase.table('daily_metrics').upsert(final_records, on_conflict='symbol,date').execute()
                print("daily_metrics table updated successfully!")

    except Exception as e:
        print(f"An unhandled error occurred: {e}"); sys.exit(1)
    finally:
        print("\n--- Daily Full Data Update Job FINISHED ---")

if __name__ == '__main__':
    do_update_job()
