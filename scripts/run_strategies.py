# scripts/run_strategies.py (优化版 - 适配40周数据)
import os
import sys
from supabase import create_client, Client
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def screen_strong_stocks(supabase: Client, target_date_str: str):
    """
    执行强势股筛选策略（13项硬性指标，调整为40周版本）
    """
    print(f"\n--- Running Strategy: [Strong Stocks - 40W Version] for date: {target_date_str} ---")
    
    try:
        # 1. 获取目标日期的metrics数据
        print("  -> Fetching daily_metrics data...")
        metrics_response = supabase.table('daily_metrics').select('*').eq('date', target_date_str).execute()
        if not metrics_response.data:
            print("  -> No daily_metrics data found. Skipping.")
            return
        df_metrics = pd.DataFrame(metrics_response.data)

        # 2. 获取bars数据
        print("  -> Fetching daily_bars data...")
        bars_response = supabase.table('daily_bars').select('symbol, date, close, volume, amount').eq('date', target_date_str).execute()
        if not bars_response.data:
            print("  -> No daily_bars data found. Skipping.")
            return
        df_bars = pd.DataFrame(bars_response.data)

        # 3. 合并数据
        print("  -> Merging data...")
        df = pd.merge(df_metrics, df_bars, on=['symbol', 'date'], how='inner')
        df.dropna(subset=['close', 'ma50'], inplace=True)  # 至少要有close和ma50
        
        if df.empty:
            print("  -> No valid data after merging. Skipping.")
            return
        
        print(f"  -> Starting with {len(df)} stocks for screening.")

        # 4. 应用13项筛选条件（调整为更宽松的标准）
        print("  -> Applying screening conditions...")
        
        # 趋势指标（4项）
        cond1 = df['close'] > df['ma50'].fillna(0)                    # 1. 股价 > 50日均线
        cond2 = df['close'] > df['ma150'].fillna(0)                   # 2. 股价 > 150日均线（可能NULL）
        cond3 = df['ma150'].fillna(0) > df['ma200'].fillna(0)         # 3. 150日线 > 200日线（宽松处理）
        cond4 = df['ma10'].fillna(0) > df['ma20'].fillna(0)           # 4. 10日线 > 20日线

        # 价格强度（4项）- 调整为40周（约200天）
        cond5 = df['close'] >= df['low_52w'].fillna(0) * 1.3          # 5. 较40周低点高30%+
        cond6 = df['close'] >= df['high_52w'].fillna(df['close']) * 0.8  # 6. 距40周高点<20%
        cond7 = df['rs_rating'].fillna(0) >= 70                       # 7. RS≥70
        cond8 = df['close'] > 10                                      # 8. 股价 > 10元

        # 流动性规模（4项）
        cond9 = df['total_market_cap'].fillna(0) > 3_000_000_000     # 9. 市值 > 30亿
        cond10 = df['volume'] > 500_000                               # 10. 成交量 > 50万
        cond11 = df['amount'] > 100_000_000                           # 11. 成交额 > 1亿（放宽）
        cond12 = (
            (df['volume_ma10'].fillna(0) > 500_000) & 
            (df['volume_ma30'].fillna(0) > 500_000) & 
            (df['volume_ma60'].fillna(0) > 500_000)
        )                                                             # 12. 平均成交量达标

        # 5. 计算每只股票满足的条件数
        df['conditions_met'] = (
            cond1.astype(int) + cond2.astype(int) + cond3.astype(int) + cond4.astype(int) +
            cond5.astype(int) + cond6.astype(int) + cond7.astype(int) + cond8.astype(int) +
            cond9.astype(int) + cond10.astype(int) + cond11.astype(int) + cond12.astype(int)
        )
        
        # 6. 筛选满足11项以上的股票
        final_selection = df[df['conditions_met'] >= 11].copy()
        final_selection = final_selection.sort_values('conditions_met', ascending=False)
        
        print(f"  -> Found {len(final_selection)} stocks that meet 11+ criteria.")
        
        # 打印详细信息
        if len(final_selection) > 0:
            print("\n  -> Top 10 stocks by conditions met:")
            for idx, row in final_selection.head(10).iterrows():
                print(f"     {row['symbol']}: {row['conditions_met']}/12 条件, RS={row.get('rs_rating', 0):.1f}")

        # 7. 准备并上传结果
        if not final_selection.empty:
            # 先清空旧结果
            supabase.table('strategy_results').delete().eq('strategy_id', 'strong_stocks_v1').eq('date', target_date_str).execute()
            
            records_to_upsert = []
            for idx, row in final_selection.iterrows():
                records_to_upsert.append({
                    'strategy_id': 'strong_stocks_v1',
                    'date': target_date_str,
                    'symbol': row['symbol'],
                    'data': {
                        'conditions_met': int(row['conditions_met']),
                        'close': float(row['close']),
                        'rs_rating': float(row.get('rs_rating', 0)),
                        'market_cap': float(row.get('total_market_cap', 0)),
                        'volume': int(row['volume']),
                        'amount': float(row['amount'])
                    }
                })
            
            print(f"  -> Upserting {len(records_to_upsert)} results to strategy_results table...")
            supabase.table('strategy_results').upsert(records_to_upsert).execute()
            print("  -> ✅ Strategy results successfully updated!")
        else:
            print("  -> ⚠️ No stocks meet the criteria. Try relaxing the conditions.")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"  -> ❌ An error occurred: {e}")

def main():
    print("--- Starting Job: [4/4] Run All Strategies ---")
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found."); sys.exit(1)
        
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    try:
        # 获取最新数据日期
        response = supabase.table('daily_metrics').select('date').order('date', desc=True).limit(1).execute()
        if not response.data:
            print("Error: No data in daily_metrics. Exiting."); sys.exit(1)
        target_date_str = response.data[0]['date']
        
        print(f"\nTarget date for strategy execution: {target_date_str}")
        
        # 运行筛选策略
        screen_strong_stocks(supabase, target_date_str)
        
    except Exception as e:
        print(f"An unhandled error occurred: {e}")
        import traceback
        traceback.print_exc()

    print("\n--- Job Finished: All Strategies Completed ---")

if __name__ == '__main__':
    main()
