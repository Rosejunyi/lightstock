# worker.py (GitHub Actions - 最终的、全功能、数据清洗版)
import os
import sys
from supabase import create_client, Client
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime

# --- 1. 从 Secrets/Env 安全加载配置 ---
# 在本地运行时，它会从 .env 读取
from dotenv import load_dotenv
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
# ------------------------------------

def get_valid_symbols_whitelist(supabase_client):
    """ 从 stocks_info 分页获取所有有效的股票 symbol 列表 """
    print("Fetching whitelist from stocks_info (with pagination)...")
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

def do_update_job():
    print("--- Starting Daily Full Data Update Job ---")
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("Error: Secrets not available."); sys.exit(1)
            
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")

        valid_symbols_whitelist = get_valid_symbols_whitelist(supabase)
        
        # 步骤 1: 更新 Daily Bars (日线行情) - 暂时留空，我们先聚焦 metrics
        print("\n--- Step 1: Updating Daily Bars (Placeholder) ---")
        # ... (未来在这里放入 daily_bars 的更新逻辑) ...
        
        # 步骤 2: 更新 Daily Metrics (每日指标)
        print("\n--- Step 2: Updating Daily Metrics ---")
        print("Fetching real-time metrics from AKShare...")
        metrics_df = ak.stock_zh_a_spot_em()
        if metrics_df is None or metrics_df.empty:
            print("Could not fetch daily metrics. Skipping.")
        else:
            print(f"Fetched {len(metrics_df)} metric records.")
            
            # --- 核心修复：替换掉所有不符合 JSON 规范的特殊浮点数 ---
            metrics_df.replace([np.inf, -np.inf], np.nan, inplace=True)
            
            records_to_upsert = []
            metrics_date = datetime.now().date().strftime('%Y-%m-%d')
            
            # 使用 .where(pd.notna(...), None) 可以优雅地将所有 NaN 转换为 None
            dict_records = metrics_df.where(pd.notna(metrics_df), None).to_dict('records')

            for row in dict_records:
                code = str(row.get('代码'))
                if not code: continue
                market = 'SH' if code.startswith(('60','68')) else 'SZ'
                symbol = f"{code}.{market}"
                
                # 使用“白名单”进行过滤
                if symbol not in valid_symbols_whitelist:
                    continue
                
                record = {
                    'symbol': symbol, 'date': metrics_date,
                    'pe_ratio_dynamic': row.get('市盈率-动态'),
                    'pb_ratio': row.get('市净率'),
                    'total_market_cap': row.get('总市值'),
                    'float_market_cap': row.get('流通市值'),
                    'turnover_rate': row.get('换手率'),
                    'ma5': None, 'ma10': None, 'rsi14': None # 暂时填充为空
                }
                
                # 安全地进行类型转换
                try:
                    if record['total_market_cap'] is not None:
                        record['total_market_cap'] = int(record['total_market_cap'])
                    if record['float_market_cap'] is not None:
                        record['float_market_cap'] = int(record['float_market_cap'])
                except (ValueError, TypeError):
                    print(f"  -> Warning: Skipping record for {symbol} due to bad market cap format.")
                    continue
                
                records_to_upsert.append(record)

            print(f"Filtered down to {len(records_to_upsert)} valid records.")
            
            if records_to_upsert:
                print(f"Upserting {len(records_to_upsert)} metric records to daily_metrics...")
                supabase.table('daily_metrics').upsert(records_to_upsert, on_conflict='symbol,date').execute()
                print("daily_metrics table updated successfully!")

    except Exception as e:
        print(f"An unhandled error occurred: {e}"); sys.exit(1)
    finally:
        print("\n--- Daily Full Data Update Job FINISHED ---")

if __name__ == '__main__':
    main()
