# scripts/run_strategies.py (完整修复版)
import os
import sys
from supabase import create_client, Client
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_latest_trading_date(supabase: Client) -> str:
    """从 daily_bars 表获取最新的交易日期"""
    try:
        response = supabase.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
        if response.data:
            return response.data[0]['date']
        else:
            response = supabase.table('daily_metrics').select('date').order('date', desc=True).limit(1).execute()
            if response.data:
                return response.data[0]['date']
            else:
                return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    except Exception as e:
        print(f"Warning: Could not get latest trading date: {e}")
        return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

def safe_float(value, default=0.0):
    """安全地转换为float"""
    if value is None or pd.isna(value):
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def safe_int(value, default=0):
    """安全地转换为int"""
    if value is None or pd.isna(value):
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def screen_strong_stocks(supabase: Client, target_date_str: str):
    """执行强势股筛选策略（12项硬性指标）"""
    print(f"\n--- Running Strategy: [Strong Stocks - 40W Version] for date: {target_date_str} ---")
    
    try:
        # 1. 获取目标日期的metrics数据
        print("  -> Fetching daily_metrics data...")
        metrics_response = supabase.table('daily_metrics').select('*').eq('date', target_date_str).execute()
        if not metrics_response.data:
            print(f"  -> ❌ No daily_metrics data found for {target_date_str}. Skipping.")
            return
        df_metrics = pd.DataFrame(metrics_response.data)
        print(f"  -> Found {len(df_metrics)} records in daily_metrics")

        # 2. 获取bars数据
        print("  -> Fetching daily_bars data...")
        bars_response = supabase.table('daily_bars').select('symbol, date, close, volume, amount').eq('date', target_date_str).execute()
        if not bars_response.data:
            print(f"  -> ❌ No daily_bars data found for {target_date_str}. Skipping.")
            return
        df_bars = pd.DataFrame(bars_response.data)
        print(f"  -> Found {len(df_bars)} records in daily_bars")

        # 3. 合并数据
        print("  -> Merging data...")
        df = pd.merge(df_metrics, df_bars, on=['symbol', 'date'], how='inner')
        df.dropna(subset=['close', 'ma50'], inplace=True)
        
        if df.empty:
            print("  -> ❌ No valid data after merging. Skipping.")
            return
        
        print(f"  -> Starting with {len(df)} stocks for screening.")

        # 4. 应用12项筛选条件
        print("  -> Applying screening conditions...")
        
        # 趋势指标（4项）
        cond1 = df['close'] > df['ma50'].fillna(0)
        cond2 = df['close'] > df['ma150'].fillna(0)
        cond3 = df['ma150'].fillna(0) > df['ma200'].fillna(0)
        cond4 = df['ma10'].fillna(0) > df['ma20'].fillna(0)

        # 价格强度（4项）
        cond5 = df['close'] >= df['low_52w'].fillna(0) * 1.3
        cond6 = df['close'] >= df['high_52w'].fillna(df['close']) * 0.8
        cond7 = df['rs_rating'].fillna(0) >= 70
        cond8 = df['close'] > 10

        # 流动性规模（4项）
        cond9 = df['total_market_cap'].fillna(0) > 3_000_000_000
        cond10 = df['volume'] > 500_000
        cond11 = df['amount'] > 100_000_000
        cond12 = (
            (df['volume_ma10'].fillna(0) > 500_000) & 
            (df['volume_ma30'].fillna(0) > 500_000) & 
            (df['volume_ma60'].fillna(0) > 500_000)
        )

        # 5. 计算每只股票满足的条件数
        df['conditions_met'] = (
            cond1.astype(int) + cond2.astype(int) + cond3.astype(int) + cond4.astype(int) +
            cond5.astype(int) + cond6.astype(int) + cond7.astype(int) + cond8.astype(int) +
            cond9.astype(int) + cond10.astype(int) + cond11.astype(int) + cond12.astype(int)
        )
        
        # 6. 筛选满足11项以上的股票
        final_selection = df[df['conditions_met'] >= 11].copy()
        final_selection = final_selection.sort_values('conditions_met', ascending=False)
        
        print(f"  -> ✅ Found {len(final_selection)} stocks that meet 11+ criteria.")
        
        # 打印详细信息
        if len(final_selection) > 0:
            print("\n  -> Top 10 stocks by conditions met:")
            for idx, row in final_selection.head(10).iterrows():
                rs = safe_float(row.get('rs_rating', 0), 0)
                close = safe_float(row['close'], 0)
                print(f"     {row['symbol']}: {row['conditions_met']}/12 条件, RS={rs:.1f}, 价格={close:.2f}")

        # 7. 准备并上传结果（使用安全转换函数）
        if not final_selection.empty:
            print(f"  -> Deleting old results for strategy_id='strong_stocks_v1' and date='{target_date_str}'...")
            supabase.table('strategy_results').delete().eq('strategy_id', 'strong_stocks_v1').eq('date', target_date_str).execute()
            
            records_to_upsert = []
            for idx, row in final_selection.iterrows():
                records_to_upsert.append({
                    'strategy_id': 'strong_stocks_v1',
                    'date': target_date_str,
                    'symbol': row['symbol'],
                    'data': {
                        'conditions_met': safe_int(row['conditions_met'], 0),
                        'close': safe_float(row['close'], 0),
                        'rs_rating': safe_float(row.get('rs_rating', 0), 0),
                        'market_cap': safe_float(row.get('total_market_cap', 0), 0),
                        'volume': safe_int(row['volume'], 0),
                        'amount': safe_float(row['amount'], 0)
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
        target_date_str = get_latest_trading_date(supabase)
        
        print(f"\n📅 Latest trading date detected: {target_date_str}")
        print(f"   (Today is {datetime.now().strftime('%Y-%m-%d')}, which may not be a trading day)")
        
        screen_strong_stocks(supabase, target_date_str)
        
    except Exception as e:
        print(f"An unhandled error occurred: {e}")
        import traceback
        traceback.print_exc()

    print("\n--- Job Finished: All Strategies Completed ---")

if __name__ == '__main__':
    main()
