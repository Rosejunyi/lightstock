# worker.py (Cron Job 版本)
import os
from supabase import create_client, Client
import akshare as ak
from datetime import datetime

# --- 配置信息 ---
SUPABASE_URL = "https://rqkdkpxdjbsxkstegjhq.supabase.co"
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', "你的service_role密钥")
# ----------------

def do_update_job():
    """ 这是真正执行数据更新的核心函数 """
    print("--- Starting data update job (Render Cron Job) ---")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        print("Fetching all stock data from AKShare...")
        stock_df = ak.stock_zh_a_spot_em()
        if stock_df is None or stock_df.empty:
            print("Failed to fetch data from AKShare.")
            return
        print(f"Fetched {len(stock_df)} records from AKShare.")
        
        records_to_upsert = []
        today = datetime.now().strftime('%Y-%m-%d')

        for index, row in stock_df.iterrows():
            code = str(row['代码'])
            open_price = float(row.get('今开', 0))
            if open_price == 0: continue
            market = ''
            if code.startswith(('60', '68')): market = 'SH'
            elif code.startswith(('00', '30')): market = 'SZ'
            if not market: continue
            records_to_upsert.append({
                "symbol": f"{code}.{market}", "date": today,
                "open": open_price, "high": float(row.get('最高', 0)),
                "low": float(row.get('最低', 0)), "close": float(row.get('最新价', 0)),
                "volume": int(row.get('成交量', 0)), "amount": float(row.get('成交额', 0)),
            })
        
        print(f"Total valid records to upsert: {len(records_to_upsert)}")
        
        if records_to_upsert:
            print("Upserting data to daily_bars table...")
            batch_size = 500
            for i in range(0, len(records_to_upsert), batch_size):
                batch = records_to_upsert[i:i+batch_size]
                supabase.table('daily_bars').upsert(batch, on_conflict='symbol,date').execute()
            print("Upsert completed successfully.")
    except Exception as e:
        print(f"An error occurred during the update job: {e}")
    finally:
        print("--- Data update job finished ---")

if __name__ == '__main__':
    do_update_job() # 直接运行任务