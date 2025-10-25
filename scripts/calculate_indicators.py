# scripts/calculate_indicators.py (完整Python实现版)
import os
import sys
from supabase import create_client, Client
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
import numpy as np

load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_latest_trading_date(supabase: Client) -> str:
    """获取最新交易日"""
    response = supabase.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
    if response.data:
        return response.data[0]['date']
    return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

def fetch_historical_bars(supabase: Client, days_back: int, target_date: str):
    """获取历史K线数据"""
    start_date = (datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=days_back * 2)).strftime('%Y-%m-%d')
    
    print(f"  -> Fetching bars from {start_date} to {target_date}...")
    all_data = []
    offset = 0
    batch_size = 1000
    
    while True:
        response = supabase.table('daily_bars')\
            .select('symbol, date, close, volume, high, low')\
            .gte('date', start_date)\
            .lte('date', target_date)\
            .order('symbol')\
            .order('date')\
            .range(offset, offset + batch_size - 1)\
            .execute()
        
        if not response.data:
            break
        
        all_data.extend(response.data)
        offset += batch_size
        
        if len(response.data) < batch_size:
            break
    
    print(f"  -> Fetched {len(all_data)} records")
    return pd.DataFrame(all_data)

def calculate_moving_averages(df: pd.DataFrame, periods: list) -> pd.DataFrame:
    """计算移动平均线"""
    print("  -> Calculating moving averages...")
    
    # 按股票分组，计算每个周期的MA
    for period in periods:
        df[f'ma{period}'] = df.groupby('symbol')['close'].transform(
            lambda x: x.rolling(window=period, min_periods=period).mean()
        )
    
    return df

def calculate_52w_high_low(df: pd.DataFrame, window: int = 200) -> pd.DataFrame:
    """计算52周（200个交易日）高低点"""
    print("  -> Calculating 52-week high/low...")
    
    df['high_52w'] = df.groupby('symbol')['high'].transform(
        lambda x: x.rolling(window=window, min_periods=window).max()
    )
    df['low_52w'] = df.groupby('symbol')['low'].transform(
        lambda x: x.rolling(window=window, min_periods=window).min()
    )
    
    return df

def calculate_volume_ma(df: pd.DataFrame, periods: list) -> pd.DataFrame:
    """计算成交量均线"""
    print("  -> Calculating volume moving averages...")
    
    for period in periods:
        df[f'volume_ma{period}'] = df.groupby('symbol')['volume'].transform(
            lambda x: x.rolling(window=period, min_periods=period).mean()
        )
    
    return df

def calculate_rs_rating_python(df: pd.DataFrame, lookback_period: int = 60) -> pd.DataFrame:
    """
    计算RS Rating（相对强度评级）
    
    第一性原理：
    1. 计算每只股票在过去N天的涨跌幅
    2. 将所有股票按涨跌幅排序
    3. 计算百分位排名（0-100）
    """
    print(f"  -> Calculating RS Rating (lookback: {lookback_period} days)...")
    
    # 为每只股票计算收益率
    def calc_return(group):
        """计算单只股票的收益率"""
        if len(group) < lookback_period:
            return np.nan
        
        # 获取最新价格和N天前的价格
        latest_close = group.iloc[-1]
        old_close = group.iloc[-lookback_period] if len(group) >= lookback_period else group.iloc[0]
        
        if old_close > 0:
            return ((latest_close - old_close) / old_close) * 100
        return np.nan
    
    # 按股票分组，计算每只股票的收益率
    df['price_return'] = df.groupby('symbol')['close'].transform(calc_return)
    
    # 对于最新日期，计算RS Rating
    latest_date = df['date'].max()
    latest_data = df[df['date'] == latest_date].copy()
    
    # 移除没有收益率数据的股票
    valid_stocks = latest_data.dropna(subset=['price_return'])
    
    if len(valid_stocks) == 0:
        print("  -> ⚠️ No valid stocks for RS Rating calculation")
        df['rs_rating'] = np.nan
        return df
    
    # 计算百分位排名（核心算法）
    valid_stocks['rs_rating'] = valid_stocks['price_return'].rank(pct=True) * 99 + 1
    valid_stocks['rs_rating'] = valid_stocks['rs_rating'].round(1)
    
    # 合并回原数据
    df = df.merge(
        valid_stocks[['symbol', 'rs_rating']],
        on='symbol',
        how='left',
        suffixes=('', '_new')
    )
    
    # 如果有新的rs_rating列，使用它
    if 'rs_rating_new' in df.columns:
        df['rs_rating'] = df['rs_rating_new']
        df.drop('rs_rating_new', axis=1, inplace=True)
    
    print(f"  -> RS Rating calculated for {len(valid_stocks)} stocks")
    print(f"  -> RS Rating range: {valid_stocks['rs_rating'].min():.1f} - {valid_stocks['rs_rating'].max():.1f}")
    print(f"  -> RS Rating median: {valid_stocks['rs_rating'].median():.1f}")
    
    return df

def update_daily_metrics(supabase: Client, df: pd.DataFrame, target_date: str):
    """更新daily_metrics表"""
    print(f"  -> Updating daily_metrics for {target_date}...")
    
    # 筛选目标日期的数据
    target_data = df[df['date'] == target_date].copy()
    
    if target_data.empty:
        print("  -> No data to update")
        return
    
    # 准备要更新的记录
    records = []
    for _, row in target_data.iterrows():
        record = {
            'symbol': row['symbol'],
            'date': row['date'],
            'ma5': float(row['ma5']) if pd.notna(row['ma5']) else None,
            'ma10': float(row['ma10']) if pd.notna(row['ma10']) else None,
            'ma20': float(row['ma20']) if pd.notna(row['ma20']) else None,
            'ma30': float(row['ma30']) if pd.notna(row['ma30']) else None,
            'ma50': float(row['ma50']) if pd.notna(row['ma50']) else None,
            'ma60': float(row['ma60']) if pd.notna(row['ma60']) else None,
            'ma120': float(row['ma120']) if pd.notna(row['ma120']) else None,
            'ma150': float(row['ma150']) if pd.notna(row['ma150']) else None,
            'ma200': float(row['ma200']) if pd.notna(row['ma200']) else None,
            'ma250': float(row['ma250']) if pd.notna(row['ma250']) else None,
            'high_52w': float(row['high_52w']) if pd.notna(row['high_52w']) else None,
            'low_52w': float(row['low_52w']) if pd.notna(row['low_52w']) else None,
            'rs_rating': float(row['rs_rating']) if pd.notna(row['rs_rating']) else None,
            'volume_ma10': int(row['volume_ma10']) if pd.notna(row['volume_ma10']) else None,
            'volume_ma30': int(row['volume_ma30']) if pd.notna(row['volume_ma30']) else None,
            'volume_ma60': int(row['volume_ma60']) if pd.notna(row['volume_ma60']) else None,
            'volume_ma90': int(row['volume_ma90']) if pd.notna(row['volume_ma90']) else None,
        }
        records.append(record)
    
    # 批量更新
    batch_size = 500
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        supabase.table('daily_metrics').upsert(batch, on_conflict='symbol,date').execute()
    
    print(f"  -> ✅ Updated {len(records)} records in daily_metrics")

def main():
    print("=" * 70)
    print("🚀 Calculate Technical Indicators (Pure Python Implementation)")
    print("=" * 70)
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Error: Supabase credentials not found")
        sys.exit(1)
    
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # 获取最新交易日
    target_date = get_latest_trading_date(supabase)
    print(f"\n📅 Target date: {target_date}")
    
    try:
        # 1. 获取历史数据（需要足够长的历史，以计算250日均线）
        df = fetch_historical_bars(supabase, days_back=300, target_date=target_date)
        
        if df.empty:
            print("❌ No data found")
            sys.exit(1)
        
        # 确保日期格式正确
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        
        # 按symbol和date排序
        df = df.sort_values(['symbol', 'date'])
        
        print(f"\n📊 Data loaded: {len(df)} records, {df['symbol'].nunique()} stocks")
        
        # 2. 计算所有移动平均线
        ma_periods = [5, 10, 20, 30, 50, 60, 120, 150, 200, 250]
        df = calculate_moving_averages(df, ma_periods)
        
        # 3. 计算52周高低点
        df = calculate_52w_high_low(df, window=200)
        
        # 4. 计算成交量均线
        volume_ma_periods = [10, 30, 60, 90]
        df = calculate_volume_ma(df, volume_ma_periods)
        
        # 5. 计算RS Rating（关键！）
        df = calculate_rs_rating_python(df, lookback_period=60)
        
        # 6. 更新数据库
        update_daily_metrics(supabase, df, target_date)
        
        print("\n" + "=" * 70)
        print("✅ All indicators calculated successfully!")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
