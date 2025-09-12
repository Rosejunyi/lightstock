# worker.py (GitHub Actions - 最终完美健壮回填版)
import os
import sys
from supabase import create_client, Client
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_last_trade_date_from_db(supabase_client):
    """ 从 daily_bars 获取最新的一个交易日 """
    try:
        # 查询数据库中已存在的最新日期
        response = supabase_client.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
        if response.data:
            # 如果有数据，就从最新日期的后一天开始
            return datetime.strptime(response.data[0]['date'], '%Y-%m-%d').date()
    except Exception as e:
        print(f"Warning: Could not get last trade date from Supabase: {e}")
    # 如果表为空或查询失败，从一个合理的历史日期开始，避免下载过多数据
    # 你可以根据需要调整这个“创世”日期
    return datetime.strptime("2024-01-01", "%Y-%m-%d").date()

def do_update_job():
    total_upserted_count = 0
    print("--- Starting data update job (GitHub Actions - Robust Backfill) ---")
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("Error: SUPABASE_URL and SUPABASE_KEY secrets must be set in GitHub repository secrets.")
            sys.exit(1)

        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        # 1. 确定需要开始回填的日期
        last_date_in_db = get_last_trade_date_from_db(supabase)
        next_date_to_fill = last_date_in_db + timedelta(days=1)
        today = datetime.now().date()

        print(f"Last date in DB is {last_date_in_db}. Starting backfill from {next_date_to_fill}.")

        # 如果起始日期已经超过今天，说明数据已经是最新，无需操作
        if next_date_to_fill > today:
            print("Data is already up to date. Job finished.")
            return

        # 2. 从 AKShare 获取完整的交易日历，找出这个范围内的所有真实交易日
        trade_date_df = ak.tool_trade_date_hist_df()
        trade_dates = {pd.to_datetime(d).date() for d in trade_date_df['trade_date']}
        
        dates_to_fetch = [d for d in sorted(list(trade_dates)) if next_date_to_fill <= d <= today]
        
        if not dates_to_fetch:
            print("No new trading days to fetch in the specified range. Job finished.")
            return
        
        print(f"Found {len(dates_to_fetch)} new trading day(s) to update: {', '.join([d.strftime('%Y-%m-%d') for d in dates_to_fetch])}")

        # 3. 从 stocks_info 获取所有有效的 symbol，作为我们的“蓝图”
        print("Fetching valid symbols from Supabase...")
        # (这里假设 stocks_info 不会超过1000行，如果超过，也需要分页)
        response = supabase.table('stocks_info').select('symbol').execute()
        valid_symbols = {item['symbol'] for item in response.data}
        print(f"Found {len(valid_symbols)} valid symbols to track.")

        # 4. 逐个交易日进行数据获取和上传
        for trade_date in dates_to_fetch:
            trade_date_str = trade_date.strftime('%Y%m%d')
            print(f"\n--- Processing date: {trade_date_str} ---")
            
            try:
                stock_df = ak.stock_zh_a_hist(symbol="all", period="daily", start_date=trade_date_str, end_date=trade_date_str, adjust="")
                if stock_df is None or stock_df.empty:
                    print(f"  -> No data returned from AKShare for {trade_date_str}.")
                    continue
                
                print(f"  -> Fetched {len(stock_df)} records from AKShare.")
                stock_df.rename(columns={'股票代码': 'code', '日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'volume', '成交额': 'amount'}, inplace=True)
                stock_df['volume'] = stock_df['volume'] * 100

                records_to_upsert = []
                for index, row in stock_df.iterrows():
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
                    print(f"  -> Upserting {len(records_to_upsert)} valid records for {trade_date_str}...")
                    supabase.table('daily_bars').upsert(records_to_upsert, on_conflict='symbol,date').execute()
                    total_upserted_count += len(records_to_upsert)
            
            except Exception as e:
                print(f"  -> An error occurred while processing {trade_date_str}: {e}")
                print("  -> Will retry this date on the next run. Stopping for now.")
                break # 遇到错误就停止本次运行，下次会自动从失败的这一天开始

    except Exception as e:
        print(f"An unhandled error occurred in background job: {e}")
        sys.exit(1)
    finally:
        print(f"\n--- Data update job FINISHED. Total new records upserted in this run: {total_upserted_count} ---")

if __name__ == '__main__':
    do_update_job()
