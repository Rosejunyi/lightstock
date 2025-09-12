# worker.py (GitHub Actions 版本)
import os
from supabase import create_client, Client
import akshare as ak
from datetime import datetime

# 从 GitHub Actions 的 Secrets 中读取配置
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def do_update_job():
    # ... (内部逻辑和我们之前 Render Cron Job 版本的完全一样) ...
    # ... (获取 AKShare 数据 -> 清洗 -> 写入 Supabase) ...

if __name__ == '__main__':
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: SUPABASE_URL and SUPABASE_KEY must be set in GitHub repository secrets.")
    else:
        do_update_job()