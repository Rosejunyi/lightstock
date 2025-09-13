# scripts/run_strategies.py
import os
import sys
from supabase import create_client, Client
import pandas as pd
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
        # 1. 获取目标日期的所有核心数据
        # 我们需要联结 daily_metrics 和 daily_bars 的数据。
        # Supabase Python 客户端不支持直接 JOIN，所以我们分别获取然后合并。
        
        # 获取 metrics 数据
        print("  -> Fetching daily_metrics data...")
        metrics_response = supabase.table('daily_metrics').select('*').eq('date', target_date_str).execute()
        if not metrics_response.data:
            print("  -> No daily_metrics data found for the target date. Skipping.")
            return
        df_metrics = pd.DataFrame(metrics_response.data)

        # 获取 bars 数据 (只需要 close, volume, amount)
        print("  -> Fetching daily_bars data...")
        bars_response = supabase.table('daily_bars').select('symbol, date, close, volume, amount').eq('date', target_date_str).execute()
        if not bars_response.data:
            print("  -> No daily_bars data found for the target date. Skipping.")
            return
        df_bars = pd.DataFrame(bars_response.data)

        # 合并数据
        print("  -> Merging metrics and bars data...")
        df = pd.merge(df_metrics, df_bars, on=['symbol', 'date'])
        
        # 过滤掉数据不完整的行
        df.dropna(inplace=True)
        if df.empty:
            print("  -> Data after merging and cleaning is empty. Skipping.")
            return
        
        print(f"  -> Starting with {len(df)} stocks for screening.")

        # 2. 将13条标准翻译成 Pandas 筛选条件
        # 趋势指标
        cond1 = df['close'] > df['ma50']
        cond2 = df['close'] > df['ma150']
        # cond3 = df['ma150'] > df['ma200'] # 暂时禁用，因为 ma200 数据不足
        cond4 = df['ma10'] > df['ma20']

        # 价格强度与位置
        cond5 = df['close'] >= df['low_52w'] * 1.3
        cond6 = df['close'] >= df['high_52w'] * 0.8
        cond7 = df['rs_rating'] >= 70
        cond8 = df['close'] > 10

        # 流动性与规模
        cond9 = df['total_market_cap'] > 3_000_000_000
        cond10 = df['volume'] > 500_000
        cond11 = df['amount'] > 100_000_000 # 成交额放宽到1亿，更容易选出
        cond12 = (df['volume_ma10'] > 500_000) & (df['volume_ma30'] > 500_000) & (df['volume_ma60'] > 500_000)

        # 3. 应用所有筛选条件
        print("  -> Applying all screening conditions...")
        final_selection = df[
            cond1 & cond2 & cond4 & cond5 & cond6 & cond7 & cond8 & cond9 & cond10 & cond11 & cond12
        ]
        
        print(f"  -> Found {len(final_selection)} stocks that meet the criteria.")

        # 4. 准备并上传结果
        if not final_selection.empty:
            records_to_upsert = []
            for index, row in final_selection.iterrows():
                records_to_upsert.append({
                    'strategy_id': 'strong_stocks_v1', # 给我们的策略一个ID
                    'date': target_date_str,
                    'symbol': row['symbol'],
                    'data': { # 使用 JSONB 字段存放一些额外信息
                        'close': row['close'],
                        'rs_rating': row['rs_rating'],
                        'market_cap': row['total_market_cap']
                    }
                })
            
            print(f"  -> Upserting {len(records_to_upsert)} results to strategy_results table...")
            # 先删除旧的策略结果，再插入新的
            supabase.table('strategy_results').delete().eq('strategy_id', 'strong_stocks_v1').eq('date', target_date_str).execute()
            supabase.table('strategy_results').upsert(records_to_upsert).execute()
            print("  -> Strategy results successfully updated!")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"  -> An error occurred during [Strong Stocks] strategy execution: {e}")

def main():
    print("--- Starting Job: [4/4] Run All Strategies ---")
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found."); sys.exit(1)
        
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 我们总是基于数据库里最新的数据日期来运行策略
    try:
        response = supabase.table('daily_metrics').select('date').order('date', desc=True).limit(1).execute()
        if not response.data:
            print("Error: No data in daily_metrics to run strategies on. Exiting."); sys.exit(1)
        target_date_str = response.data[0]['date']
        
        # 运行我们所有的策略函数
        screen_strong_stocks(supabase, target_date_str)
        # 未来可以在这里添加更多的策略函数调用，比如 screen_value_stocks(...)
        
    except Exception as e:
        print(f"An unhandled error occurred in main strategy runner: {e}")

    print("\n--- Job Finished: Run All Strategies ---")

if __name__ == '__main__':
    main()
