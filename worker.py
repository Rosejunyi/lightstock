# worker.py (最终免费版 - 蚂蚁搬家策略)
import os
import time
from flask import Flask, jsonify
from supabase import create_client, Client
import akshare as ak
from datetime import datetime
import threading

app = Flask(__name__)

# --- 配置信息 ---
SUPABASE_URL = "https://rqkdkpxdjbsxkstegjhq.supabase.co"
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', "你的service_role密钥")
# ----------------

def get_all_symbols_from_supabase(supabase_client):
    """ 从 stocks_info 分页获取所有股票 symbol """
    all_symbols = []
    page = 0
    page_size = 1000
    while True:
        start_index = page * page_size
        response = supabase_client.table('stocks_info').select('symbol').range(start_index, start_index + page_size - 1).execute()
        if not response.data:
            break
        all_symbols.extend([item['symbol'] for item in response.data])
        if len(response.data) < page_size:
            break
        page += 1
    return all_symbols

def do_update_job():
    """ 在后台线程中，分批次、慢速地更新数据 """
    print("--- Background data update job STARTED (Ant Moving Strategy) ---")
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        symbols_to_update = get_all_symbols_from_supabase(supabase)
        if not symbols_to_update:
            print("No symbols foun