# scripts/calculate_indicators.py (最终的、SQL总指挥版)
import os, sys
from supabase import create_client, Client
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def main():
    print("--- Starting Job: [3/3] Triggering SQL Indicator Calculations ---")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    today_str = datetime.now().date().strftime('%Y-%m-%d')
    
    # 依次调用所有 SQL 计算函数
    functions_to_run = ['calculate_all_ma', 'calculate_all_rsi', 'calculate_all_macd']
    
    for func_name in functions_to_run:
        print(f"  -> Calling database function: {func_name} for date {today_str}...")
        try:
            supabase.rpc(func_name, {'target_date': today_str}).execute()
            print(f"    -> {func_name} executed successfully!")
        except Exception as e:
            print(f"    -> Error calling {func_name}: {e}")
            # 即使一个失败了，也继续尝试下一个
    
    print("--- Job Finished: All SQL calculation triggers sent. ---")

if __name__ == '__main__':
    main()
