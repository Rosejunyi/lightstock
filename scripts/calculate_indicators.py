# scripts/calculate_indicators.py (优化版 - 使用新的SQL函数)
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
    
    # 🔥 使用新的一键计算函数
    try:
        print(f"  -> Calling comprehensive indicator function for date: {today_str}")
        result = supabase.rpc('calculate_all_indicators_v2', {'target_date': today_str}).execute()
        print(f"    ✅ All indicators calculated successfully!")
        
    except Exception as e:
        print(f"    ❌ Calculation failed: {e}")
        
        # 如果一键函数失败，回退到单独调用
        print("  -> Falling back to individual function calls...")
        functions_to_run = [
            'calculate_short_ma',
            'calculate_mid_ma', 
            'calculate_long_ma',
            'calculate_52w_high_low',
            'calculate_rs_rating',
            'calculate_volume_ma',
        ]
        
        for func_name in functions_to_run:
            try:
                print(f"  -> Calling {func_name}...")
                supabase.rpc(func_name, {'target_date': today_str}).execute()
                print(f"    ✅ {func_name} OK")
            except Exception as func_error:
                print(f"    ❌ {func_name} failed: {func_error}")
                continue
    
    print("--- Job Finished: Indicator calculations completed. ---")

if __name__ == '__main__':
    main()
