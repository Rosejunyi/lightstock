# worker.py
import os
from flask import Flask, jsonify
from supabase import create_client, Client
import akshare as ak
import pandas as pd
from datetime import datetime
import threading

app = Flask(__name__)

# --- 配置信息 ---
SUPABASE_URL = "https://rqkdkpxdjbsxkstegjhq.supabase.co"
# 注意：在 Render 上，我们会通过环境变量来设置这些，但在本地测试时可以直接写
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', "你的service_role密钥")
# ----------------

def do_update_job():
    """ 这是真正执行数据更新的核心函数 """
    print("--- Starting data update job ---")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")

        # 1. 获取全量行情数据
        print("Fetching all stock data from AKShare...")
        stock_df = ak.stock_zh_a_spot_em()
        if stock_df is None or stock_df.empty:
            print("Failed to fetch data from AKShare.")
            return

        print(f"Fetched {len(stock_df)} records from AKShare.")

        # 2. 数据清洗和格式化
        stock_df.rename(columns={'代码': 'code', '名称': 'name'}, inplace=True)
        
        records_to_upsert = []
        today = datetime.now().strftime('%Y-%m-%d')

        for index, row in stock_df.iterrows():
            code = str(row['code'])
            open_price = float(row.get('今开', 0))

            if open_price == 0:
                continue

            market = ''
            if code.startswith(('60', '68')): market = 'SH'
            elif code.startswith(('00', '30')): market = 'SZ'
            
            if not market:
                continue

            records_to_upsert.append({
                "symbol": f"{code}.{market}",
                "date": today,
                "open": open_price,
                "high": float(row.get('最高', 0)),
                "low": float(row.get('最低', 0)),
                "close": float(row.get('最新价', 0)),
                "volume": int(row.get('成交量', 0)),
                "amount": float(row.get('成交额', 0)),
            })
        
        print(f"Total valid records to upsert: {len(records_to_upsert)}")

        # 3. 分批写入数据库
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

@app.route('/start-update', methods=['POST'])
def trigger_update():
    # 我们在一个新的线程中运行耗时的任务，以立即返回响应
    thread = threading.Thread(target=do_update_job)
    thread.start()
    return jsonify({"message": "Update job started in background."}), 202

if __name__ == '__main__':
    # Render 会使用 Gunicorn 来运行，这一部分主要用于本地测试
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))