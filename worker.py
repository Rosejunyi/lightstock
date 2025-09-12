# worker.py (GitHub Actions - 最终的“表结构对齐”版)
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
        
        # ... (第一步：更新 Daily Bars 的逻辑完全不变) ...
        print("Daily bars update finished or is up to date.")

        # 第二步：获取并更新每日指标 (daily_metrics)
        print("\n--- Step 2: Updating Daily Metrics ---")
        print("Fetching real-time metrics from AKShare...")
        metrics_df = ak.stock_zh_a_spot_em()
        if metrics_df is None or metrics_df.empty:
            print("Could not fetch daily metrics. Skipping.")
        else:
            print(f"Fetched {len(metrics_df)} metric records.")
            
            # 替换无穷大值为 NaN
            metrics_df.replace([np.inf, -np.inf], np.nan, inplace=True)
            
            records_to_upsert = []
            metrics_date = datetime.now().date().strftime('%Y-%m-%d')
            
            # 将 DataFrame 转换为字典列表
            dict_records = metrics_df.to_dict('records')

            for row in dict_records:
                code = str(row.get('代码'))
                if not code: continue

                market = 'SH' if code.startswith(('60','68')) else 'SZ'
                symbol = f"{code}.{market}"
                
                # --- 关键修复：让数据结构与数据库表完全匹配 ---
                record = {
                    'symbol': symbol,
                    'date': metrics_date,
                    
                    # 使用 .get() 并提供 None 作为默认值，处理可能不存在的列
                    # 同时用 try-except 包裹，确保类型转换不会因坏数据而崩溃
                    'pe_ratio_dynamic': None,
                    'pb_ratio': None,
                    'total_market_cap': None,
                    'float_market_cap': None,
                    'turnover_rate': None,

                    # 为尚未计算的技术指标提供 NULL 值
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
                    # 如果任何一个值无法转换，就跳过这条记录
                    print(f"  -> Warning: Skipping record for {symbol} due to bad data format.")
                    continue
                
                records_to_upsert.append(record)

            if records_to_upsert:
                print(f"Upserting {len(records_to_upsert)} metric records to daily_metrics...")
                # 在上传前，最后一次清理 None 值，supabase-py 需要这个
                final_records = [{k: v for k, v in r.items() if v is not None} for r in records_to_upsert]
                
                supabase.table('daily_metrics').upsert(final_records, on_conflict='symbol,date').execute()
                print("daily_metrics table updated successfully!")

    except Exception as e:
        print(f"An unhandled error occurred: {e}"); sys.exit(1)
    finally:
        print("\n--- Daily Full Data Update Job FINISHED ---")

if __name__ == '__main__':
    do_update_job()
