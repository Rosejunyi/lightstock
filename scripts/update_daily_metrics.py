# scripts/update_daily_metrics.py (最终的、作用域和 Baostock 修复版)
import os
import sys
from supabase import create_client, Client
import baostock as bs
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# --- 1. 将配置加载放在文件的最顶层，让它们成为真正的全局常量 ---
load_dotenv() # 在本地运行时，这行会生效；在 Actions 里，它什么也不做，很安全
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
# -----------------------------------------------------------

def get_valid_symbols_whitelist(supabase_client: Client) -> set:
    # ... (这个函数内容是正确的，保持不变) ...
    
def main(supabase_url: str, supabase_key: str):
    print("--- Starting Job: [2/3] Update Daily Metrics (Baostock Version) ---")
    # ... (函数内部的逻辑，和我们之前修复好的 Baostock 版本完全一样) ...
    # ... (它会使用传入的 supabase_url 和 supabase_key) ...
        
# ==========================================================
# 关键修复：确保在调用 main 之前，全局变量已经被 Actions 的 env 注入
# ==========================================================
if __name__ == '__main__':
    # 现在，当脚本执行到这里时，SUPABASE_URL 和 SUPABASE_KEY 已经被正确加载
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found in environment or .env file.")
        sys.exit(1)
    # 把加载好的全局变量，作为参数传递给 main 函数
    main(SUPABASE_URL, SUPABASE_KEY)
```

**为了让你更方便，我直接提供一个整合了所有修复的、最终的 `update_daily_metrics.py` 全文:**

```python
# scripts/update_daily_metrics.py (最终的、完整的、王者归来版)
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
    
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Baostock login failed: {lg.error_msg}"); sys.exit(1)
    print("Baostock login successful.")

    try:
        metrics_date = datetime.now().date()
        date_str_for_db = metrics_date.strftime('%Y-%m-%d')
            
        print(f"Fetching latest daily metrics for {len(valid_symbols_whitelist)} stocks...")
        
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
                pe = float(row['peTTM']) if row['peTTM'] and row['peTTM'] != '' else None
                pb = float(row['pbMRQ']) if row['pbMRQ'] and row['pbMRQ'] != '' else None
                total_market_cap = int(float(row['marketValue']) * 10000) if row['marketValue'] and row['marketValue'] != '' else None
                float_market_cap = int(float(row['flowValue']) * 10000) if row['flowValue'] and row['flowValue'] != '' else None
                turnover_rate = float(row['turnoverRatio']) if row['turnoverRatio'] and row['turnoverRatio'] != '' else None

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

if __name__ == '__main__':
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found in environment or .env file.")
        sys.exit(1)
    main(SUPABASE_URL, SUPABASE_KEY)
