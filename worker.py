# worker.py (最终 Web Service 版本)
import os
from flask import Flask, jsonify
from supabase import create_client, Client
import akshare as ak
import pandas as pd
from datetime import datetime
import threading

app = Flask(__name__)

SUPABASE_URL = "https://rqkdkpxdjbsxkstegjhq.supabase.co"
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

def do_update_job():
    # ... (这个函数的内容和之前 Cron Job 版本完全一样) ...
    print("--- Starting data update job (Render Web Service) ---")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        print("Fetching all stock data from AKShare...")
        stock_df = ak.stock_zh_a_spot_em()
        if stock_df is None or stock_df.empty:
            print("Failed to fetch data from AKShare."); return
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
        print(f"An error occurred: {e}")
    finally:
        print("--- Data update job finished ---")

@app.route('/start-update', methods=['POST'])
def trigger_update():
    # 异步执行，立即返回，防止 Render 健康检查超时
    thread = threading.Thread(target=do_update_job)
    thread.start()
    return jsonify({"message": "Update job started."}), 202
    
@app.route('/')
def health_check():
    # 增加一个根路径，用于响应 Render 的健康检查
    return "OK", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)