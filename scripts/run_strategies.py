# scripts/run_strategies.py (最终实战版 - 灵活筛选)
import os
import sys
from supabase import create_client, Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv

# --- 1. 配置加载 ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def screen_strong_stocks(supabase: Client, target_date_str: str):
    """
    执行强势股筛选策略，并将结果写入 strategy_results 表。
    """
    print(f"\n--- Running Strategy: [Strong Stocks] for date: {target_date_str} ---")
    
    try:
        # 1. 获取数据
        print("  -> Fetching daily_metrics data...")
        metrics_response = supabase.table('daily_metrics').select('*').eq('date', target_date_str).execute()
        if not metrics_response.data:
            print("  -> No daily_metrics data found. Skipping.")
            return
        df_metrics = pd.DataFrame(metrics_response.data)

        print("  -> Fetching daily_bars data...")
        bars_response = supabase.table('daily_bars').select('symbol, date, close, volume, amount').eq('date', target_date_str).execute()
        if not bars_response.data:
            print("  -> No daily_bars data found. Skipping.")
            return
        df_bars = pd.DataFrame(bars_response.data)

        print("  -> Merging metrics and bars data...")
        df = pd.merge(df_metrics, df_bars, on=['symbol', 'date'])
        
        if df.empty:
            print("  -> No data after merging. Skipping.")
            return
        
        print(f"  -> Starting with a pool of {len(df)} stocks for screening.")

        # ===> [核心进化] 我们不再粗暴地dropna，而是让每个条件自我过滤 <===
        # Pandas的比较操作会自动处理NaN值（结果为False），这正是我们需要的！
        # 我们先对必要的列进行数据类型转换和净化
        
        required_cols = ['close', 'ma10', 'ma20', 'ma50', 'ma150', 'high_52w', 'low_52w', 'rs_rating', 'total_market_cap', 'volume', 'amount', 'volume_ma10', 'volume_ma30', 'volume_ma60']
        for col in required_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 2. 应用所有筛选条件
        # 我们将所有条件放在一个大的布尔表达式中。如果某行在某个指标上是NaN，该行的比较结果就是False。
        print("  -> Applying all screening conditions...")
        
        # 为了防止因为缺少某个列而报错，我们先用一个全True的Series作为起点
        mask = pd.Series(True, index=df.index)

        # 逐个应用条件，如果列存在的话
        if 'close' in df.columns and 'ma50' in df.columns: mask &= (df['close'] > df['ma50'])
        if 'close' in df.columns and 'ma150' in df.columns: mask &= (df['close'] > df['ma150'])
        if 'ma10' in df.columns and 'ma20' in df.columns: mask &= (df['ma10'] > df['ma20'])
        if 'close' in df.columns and 'low_52w' in df.columns: mask &= (df['close'] >= df['low_52w'] * 1.3)
        if 'close' in df.columns and 'high_52w' in df.columns: mask &= (df['close'] >= df['high_52w'] * 0.8)
        if 'rs_rating' in df.columns: mask &= (df['rs_rating'] >= 70)
        if 'close' in df.columns: mask &= (df['close'] > 10)
        if 'total_market_cap' in df.columns: mask &= (df['total_market_cap'] > 3_000_000_000)
        if 'volume' in df.columns: mask &= (df['volume'] > 500_000)
        if 'amount' in df.columns: mask &= (df['amount'] > 100_000_000)
        if all(c in df.columns for c in ['volume_ma10', 'volume_ma30', 'volume_ma60']):
            mask &= (df['volume_ma10'] > 500_000) & (df['volume_ma30'] > 500_000) & (df['volume_ma60'] > 500_000)

        final_selection = df[mask].copy()
        
        print(f"  -> Found {len(final_selection)} stocks that meet the criteria.")

        # 3. 准备并上传结果
        if not final_selection.empty:
            records_to_upsert = []
            for index, row in final_selection.iterrows():
                records_to_upsert.append({
                    'strategy_id': 'strong_stocks_v1',
                    'date': target_date_str,
                    'symbol': row['symbol'],
                    'data': {
                        'close': row.get('close'),
                        'rs_rating': row.get('rs_rating'),
                        'market_cap': row.get('total_market_cap')
                    }
                })
            
            print(f"  -> Upserting {len(records_to_upsert)} results to strategy_results table...")
            supabase.table('strategy_results').delete().eq('strategy_id', 'strong_stocks_v1').eq('date', target_date_str).execute()
            supabase.table('strategy_results').upsert(records_to_upsert).execute()
            print("  -> Strategy results successfully updated!")

    except Exception as e:
        import traceback
        traceback.print_exc()

def main():
    # ... main 函数完全不变 ...
    print("--- Starting Job: [4/4] Run All Strategies ---")
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found."); sys.exit(1)
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    try:
        response = supabase.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
        if not response.data:
            print("Error: No data in daily_bars to run strategies on. Exiting."); sys.exit(1)
        target_date_str = response.data[0]['date']
        print(f"  -> Setting strategy target date based on latest daily_bars: {target_date_str}")
        screen_strong_stocks(supabase, target_date_str)
    except Exception as e:
        print(f"An unhandled error occurred in main strategy runner: {e}")
    print("\n--- Job Finished: Run All Strategies ---")

if __name__ == '__main__':
    main()
