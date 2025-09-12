# worker.py (GitHub Actions - 最终完美、安全创世日期版)
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
        response = supabase_client.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
        if response.data:
            return datetime.strptime(response.data[0]['date'], '%Y-%m-%d').date()
    except Exception as e:
        print(f"Warning: Could not get last trade date: {e}")
    
    # --- 关键修复：使用更安全的“创世日期” ---
    print("  -> Could not find last date in DB, using a safe default start date.")
    return datetime.strptime("2025-01-01", "%Y-%m-%d").date()

def do_update_job():
    total_upserted_count = 0
    print("--- Starting data update job (GitHub Actions - Final Robust Version) ---")
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("Error: Secrets not available."); sys.exit(1)

        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        last_date_in_db = get_last_trade_date_from_db(supabase)
        next_date_to_fill = last_date_in_db + timedelta(days=1)
        # 我们检查到“昨天”的数据，因为“今天”的数据可能在运行时还没发布
        # 这样做也避免了在周末运行时，把 today 设置为一个非交易日
        process_until_date = datetime.now().date() - timedelta(days=1)

        print(f"Last date in DB is {last_date_in_db}. Starting backfill from {next_date_to_fill} up to {process_until_date}.")

        if next_date_to_fill > process_until_date:
            print("Data is already up to date. Job finished."); return

        # 使用最新的交易日历函数
        print("Fetching trading calendar from AKShare...")
        trade_date_df = ak.tool_trade_date_hot_df()
        trade_dates = {pd.to_datetime(d).date() for d in trade_date_df['datetime']}
        
        # 筛选出我们需要处理的日期
        dates_to_fetch = [d for d in sorted(list(trade_dates)) if next_date_to_fill <= d <= process_until_date]
        
        if not dates_to_fetch:
            print("No new trading days to fetch in the specified range. Job finished."); return
        
        print(f"Found {len(dates_to_fetch)} new trading day(s) to update: {', '.join([d.strftime('%Y-%m-%d') for d in dates_to_fetch])}")

        print("Fetching valid symbols from Supabase...")
        response = supabase.table('stocks_info').select('symbol').execute()
        valid_symbols = {item['symbol'] for item in response.data}
        print(f"Found {len(valid_symbols)} valid symbols to track.")

        # 逐个交易日进行数据获取和上传
        for trade_date in dates_to_fetch:
            trade_date_str = trade_date.strftime('%Y%m%d')
            print(f"\n--- Processing date: {trade_date_str} ---")
            
            try:
                stock_df = ak.stock_zh_a_hist(symbol="all", period="daily", start_date=trade_date_str, end_date=trade_date_str, adjust="")
                if stock_df is None or stock_df.empty:
                    print(f"  -> No data from AKShare for {trade_date_str}."); continue
                
                print(f"  -> Fetched {len(stock_df)} records.")
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
                    print(f"  -> Upserting {len(records_to_upsert)} records...")
                    supabase.table('daily_bars').upsert(records_to_upsert, on_conflict='symbol,date').execute()
                    total_upserted_count += len(records_to_upsert)
            
            except Exception as e:
                print(f"  -> An error occurred while processing {trade_date_str}: {e}")
                print("  -> Will retry this date on the next run. Stopping for now.")
                break

    except Exception as e:
        print(f"An unhandled error occurred in background job: {e}"); sys.exit(1)
    finally:
        print(f"\n--- Data update job FINISHED. Total new records upserted in this run: {total_upserted_count} ---")

if __name__ == '__main__':
    do_update_job()
