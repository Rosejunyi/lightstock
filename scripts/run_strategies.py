# scripts/run_strategies.py (最终健壮版 - 净化数据以防止TypeError)
import os
import sys
from supabase import create_client, Client
import pandas as pd
import numpy as np # 需要 numpy 来处理无穷大
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
        # 1. 获取数据 (逻辑不变)
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

        # ===> [核心修复] 在筛选前，对所有可能参与比较的列进行“净化” <===
        # 定义需要进行比较的指标列
        compare_cols = ['ma10', 'ma20', 'ma50', 'ma150', 'high_52w', 'low_52w', 'rs_rating', 'total_market_cap', 'volume', 'amount', 'volume_ma10', 'volume_ma30', 'volume_ma60']
        for col in compare_cols:
            if col in df.columns:
                # 将列的数据类型统一为数值型，无法转换的变为 NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 2. 将13条标准翻译成 Pandas 筛选条件
        # 我们现在可以安全地进行比较了，因为所有 None 都被 coerce 成了 NaN，Pandas 会自动处理
        
        # 趋势指标
        cond1 = df['close'] > df['ma50']
        cond2 = df['close'] > df['ma150']
        cond4 = df['ma10'] > df['ma20']

        # 价格强度与位置
        cond5 = df['close'] >= df['low_52w'] * 1.3
        cond6 = df['close'] >= df['high_52w'] * 0.8
        cond7 = df['rs_rating'] >= 70
        cond8 = df['close'] > 10

        # 流动性与规模
      
