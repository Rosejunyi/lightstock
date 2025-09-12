# worker.py (GitHub Actions - 最终健壮回填版)
import os
import sys
from supabase import create_client, Client
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_last_trade_date_from_db(supabase_client):
    try:
        response = supabase_client.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
        if response.data:
            return datetime.strptime(response.data[0]['date'], '%Y-%m-%d').date()
    except Exception as e:
        print(f"Warning: Could not get last trade date: {e}")
    # 如果表为空，从一个合理的历史日期开始
    return datetime.strptime("2024-01-01", "%Y-%m-%d").date()

def do_update_job():
    total_upserted_count = 0
    print("--- Starting data update job (GitHub Actions - Robust Backfill) ---")
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("Error: Secrets not available."); sys.exit(1)

        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        # 1. 确定需要开始回填的日期
        next_date_to_fill = get_last_trade_date_from_db(supabase) + timedelta(days=1)
        today = datetime.now().date()

        print(f"Last date in DB: {next_date_to_fill - timedelta(days=1)}. Starting backfill from: {next_date_to_fill}")

        # 2. 从 AKShare 获取完整的交易日历
        trade_date_df = ak.tool_trade_date_hist_df()
        trade_dates = {pd.to_datetime(d).date() for d in trade_date_df['trade_date']}

        # 3. 从 stocks_info 获取“蓝图”
        print("Fetching valid symbols from Supabase...")
        response = supabase.table('stocks_info').select('symbol').execute()
        valid_symbols = {item['symbol'] for item in response.data}
        print(f"Found {len(valid_symbols)} valid symbols.")

        # 4. 循环，一天一天地追赶，直到今天
        while next_date_to_fill <= today:
            # 检查当天是否是交易日
            if next_date_to_fill not in trade_dates:
                print(f"\nSkipping {next_date_to_fill}: Not a trading day.")
                next_date_to_fill += timedelta(days=1)
                continue

            trade_date_str = next_date_to_fill.strftime('%Y%m%d')
            print(f"\n--- Processing date: {trade_date_str} ---")
            
            try:
                stock_df = ak.stock_zh_a_hist(symbol="all", period="daily", start_date=trade_date_str, end_date=trade_date_str, adjust="")
                if stock_df is None or stock_df.empty:
                    print(f"  -> No data from AKShare for {trade_date_str}."); 
                    next_date_to_fill += timedelta(days=1)
                    continue
                
                print(f"  -> Fetched {len(stock_df)} records.")
                stock_df.rename(columns={'股票代码': 'code', '日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'volume', '成交额': 'amount'}, inplace=True)
                stock_df['volume'] = stock_df['volume'] * 100

                records_to_upsert = []
                for index, row in stock_df.iterrows():
                    # ... (数据清洗和过滤逻辑和之前一样)
                    code = str(row['code'])
                    market = ''
                    if code.startswith(('60', '68')): market = 'SH'
                    elif code.startswith(('00', '30')): market = 'SZ'
                    if not market: continue
                    symbol = f"{code}.{market}"
                    if symbol in valid_symbols:
                        records_to_upsert.append({
                            "symbol": symbol, "date": row['date'], "open": row['open'], "high": row['high'],
                            "low": row['low'], "close": row['close'], "volume": int(row['volume']), "amount": int(row['amount']),
                        })
                
                if records_to_upsert:
                    print(f"  -> Upserting {len(records_to_upsert)} records...")
                    supabase.table('daily_bars').upsert(records_to_upsert, on_conflict='symbol,date').execute()
                    total_upserted_count += len(records_to_upsert)
                
                # 成功后，日期前进一天
                next_date_to_fill += timedelta(days=1)

            except Exception as e:
                print(f"  -> An error occurred while processing {trade_date_str}: {e}")
                print("  -> Will retry on the next 
