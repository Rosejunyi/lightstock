# scripts/calculate_indicators.py (王者归来版 - 严格遵循 work.py 成功模式)
import os
import sys
from supabase import create_client, Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
from MyTT import *

# --- 1. 配置加载 ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# --- 2. 核心策略参数 ---
STRATEGY_PERIOD = 120 
# ===> 采纳 work.py 的核心秘诀：分页大小 <===
PAGINATION_SIZE = 1000 
UPSERT_BATCH_SIZE = 200 # 用于上传的分批大小
print(f"\n!!! RUNNING THE KING'S RETURN EDITION (Pagination: {PAGINATION_SIZE}) !!!\n")

def main():
    print("--- Starting Job: [3/3] Calculate All Indicators ---")
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Supabase credentials not found."); sys.exit(1)
        
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # --- 步骤 1: 确定目标日期 ---
    try:
        latest_bar_response = supabase.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
        if not latest_bar_response.data:
            print("Error: No data in daily_bars table. Exiting."); sys.exit(1)
        target_date_str = latest_bar_response.data[0]['date']
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        print(f"  -> Target date for calculation is: {target_date_str}")
    except Exception as e:
        print(f"  -> Error determining target date: {e}. Exiting."); sys.exit(1)

    # ===> [核心重构] 完全采纳 work.py 的“无差别”分页下载模式 <===
    print("\n--- 步骤 2: 通过分页获取全部所需历史数据 ---")
    all_historical_data = []
    page = 0
    start_date_fetch = (target_date - timedelta(days=STRATEGY_PERIOD + 60)).strftime('%Y-%m-%d')
    while True:
        print(f"  -> 正在获取第 {page+1} 页数据...")
        # 我们不再筛选 symbol，而是获取时间范围内的所有数据
        response = supabase.table('daily_bars') \
            .select('symbol, date, open, high, low, close, volume') \
            .gte('date', start_date_fetch) \
            .lte('date', target_date_str) \
            .order('date', desc=False) \
            .range(page * PAGINATION_SIZE, (page + 1) * PAGINATION_SIZE - 1).execute()
        
        if not response.data: break
        all_historical_data.extend(response.data)
        if len(response.data) < PAGINATION_SIZE: break
        page += 1

    if not all_historical_data:
        print("--- 在所需时间范围内未找到任何历史数据。任务结束。 ---")
        return

    df = pd.DataFrame(all_historical_data)
    df['date'] = pd.to_datetime(df['date']).dt.date
    print(f"  -> 成功获取了总计 {len(df)} 行数据。")

    # --- 步骤 3: 在一个强大的操作中计算所有指标 ---
    def calculate_all_indicators(group):
        # 采纳 work.py 的稳健模式：数据不足则返回原始组
        if len(group) < 30: return group 
        
        CLOSE = group['close']; HIGH = group['high']; LOW = group['low']; VOLUME = group['volume']
        
        # 计算所有短期指标
        group['ma10'] = MA(CLOSE, 10); group['ma20'] = MA(CLOSE, 20)
        group['ma50'] = MA(CLOSE, 50); group['ma60'] = MA(CLOSE, 60)
        group['volume_ma10'] = VOLUME.rolling(window=10).mean(); group['volume_ma30'] = VOLUME.rolling(window=30).mean()
        group['volume_ma60'] = VOLUME.rolling(window=60).mean(); group['volume_ma90'] = VOLUME.rolling(window=90).mean()
        DIF, DEA, _ = MACD(CLOSE.values); group['macd_diff'] = DIF; group['macd_dea'] = DEA
        group['rsi14'] = RSI(CLOSE.values, 14)
        
        # 只有在数据足够时才计算长周期指标
        if len(group) >= STRATEGY_PERIOD:
            group['ma150'] = MA(CLOSE, 150)
            group['high_52w'] = HIGH.rolling(window=STRATEGY_PERIOD, min_periods=1).max()
            group['low_52w'] = LOW.rolling(window=STRATEGY_PERIOD, min_periods=1).min()
            
            # RS评分的原始值计算被无缝整合进来
            start_price = group['close'].iloc[-STRATEGY_PERIOD]
            end_price = group['close'].iloc[-1]
            group['rs_raw'] = (end_price / start_price - 1) if start_price != 0 else 0
        
        return group

    print("\n--- 正在对整个数据集应用计算... ---")
    df_with_ta = df.groupby('symbol', group_keys=False).apply(calculate_all_indicators)
    print("  -> 所有计算已应用。")

    # --- 步骤 4: 筛选、排名并准备上传 ---
    print("\n--- 正在筛选目标日期的结果... ---")
    today_indicators = df_with_ta[df_with_ta['date'] == target_date].copy()
    
    # 计算最终的RS评分 (0-100的百分位排名)
    if 'rs_raw' in today_indicators.columns:
        # 过滤掉无法计算RS的行（NaN值）
        valid_rs = today_indicators.dropna(subset=['rs_raw'])
        today_indicators['rs_rating'] = valid_rs['rs_raw'].rank(pct=True) * 100
        print("  -> RS评分已计算。")

    if today_indicators.empty:
        print("--- 目标日期没有任何指标需要更新。任务结束。 ---")
        return

    print(f"  -> 找到 {len(today_indicators)} 只股票在目标日期有数据。")
    
    records_to_upsert = []
    indicator_columns = ['ma10', 'ma20', 'ma50', 'ma60', 'ma150', 'high_52w', 'low_52w', 'volume_ma10', 'volume_ma30', 'volume_ma60', 'volume_ma90', 'macd_diff', 'macd_dea', 'rsi14', 'rs_rating']
    for index, row in today_indicators.iterrows():
        record = {'symbol': row['symbol'], 'date': row['date'].strftime('%Y-%m-%d')}
        for col in indicator_columns:
            # 采纳 work.py 的安全模式 .get()
            if pd.notna(row.get(col)):
                record[col] = float(row.get(col))
        if len(record) > 2: # 确保至少有一个有效指标
            records_to_upsert.append(record)

    # --- 步骤 5: 最终上传 ---
    if records_to_upsert:
        print(f"\n--- 准备上传总计 {len(records_to_upsert)} 条计算记录 ---")
        for i in range(0, len(records_to_upsert), UPSERT_BATCH_SIZE):
            upload_batch = records_to_upsert[i:i+UPSERT_BATCH_SIZE]
            print(f"    -> 正在上传块 {i//UPSERT_BATCH_SIZE + 1} ({len(upload_batch)} 条记录)...")
            supabase.table('daily_metrics').upsert(upload_batch, on_conflict='symbol,date').execute()
        print("  -> 所有指标已成功更新！")
    else:
        print("\n--- 没有计算出新的指标需要上传。 ---")
        
    print("\n--- 任务圆满完成 ---")

if __name__ == '__main__':
    main()
