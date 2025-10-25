# scripts/calculate_indicators.py (完整版 - 包含所有指标)
import os, sys
from supabase import create_client, Client
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def main():
    print("--- Starting Job: [3/4] Triggering SQL Indicator Calculations ---")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    today_str = datetime.now().date().strftime('%Y-%m-%d')
    
    # 依次调用所有 SQL 计算函数
    functions_to_run = [
        # 原有的函数
        'calculate_short_ma',      # 短期均线 (5/10/20日)
        'calculate_mid_ma',        # 中期均线 (30/60日)
        'calculate_long_ma',       # 长期均线 (120/200日)
        'calculate_all_rsi',       # RSI指标
        'calculate_all_macd',      # MACD指标
        
        # 🔥 新增的函数（13项筛选所需）
        'calculate_52w_high_low',  # 52周高低点
        'calculate_rs_rating',     # 相对强度RS
        'calculate_volume_ma',     # 成交量均线
    ]
    
    for func_name in functions_to_run:
        try:
            print(f"  -> Calling database function: {func_name}...")
            supabase.rpc(func_name, {'target_date': today_str}).execute()
            print(f"    ✅ {func_name} executed successfully!")
        except Exception as e:
            print(f"    ❌ {func_name} failed: {e}")
            # 继续执行其他函数，不中断
            continue
    
    print("--- Job Finished: All SQL calculation triggers sent. ---")

if __name__ == '__main__':
    main()
