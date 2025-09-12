# worker.py (GitHub Actions - 最终完美回填版)
import os
import sys
from supabase import create_client, Client
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_last_trade_date(supabase_client):
    """ 从 daily_bars 获取最新的一个交易日 """
    try:
        response = supabase_client.table('daily_bars').select('date').order('date', desc=True).limit(1).execute()
        if response.data:
            return datetime.strptime(response.data[0]['date'], '%Y-%m-%d')
    except Exception as e:
        print(f"Warning: Could not get last trade date from Supabase: {e}")
    # 如果表为空或查询失败，返回一个默认的起始日期
    return datetime.strptime("2024-01-01", "%Y-%m-%d")


def do_update_job():
    print("--- Starting data update job (GitHub Actions - Perfect Backfill) ---")
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("Error: SUPABASE_URL and SUPABASE_KEY secrets are not available.")
            sys.exit(1)

        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
        
        # 1. 确定需要回填的日期范围
        last_date = get_last_trade_date(supabase)
        start_date = last_date + timedelta(days=1)
        end_date = datetime.now()

        # 如果起始日期已经超过今天，说明数据已经是最新，无需操作
        if start_date.date() > end_date.date():
            print(f"Data is already up to date (last date: {last_date.strftime('%Y-%m-%d')}). Job finished.")
            return

        start_date_str = start_date.strftime('%Y%m%d')
        end_date_str = end_date.strftime('%Y%m%d')
        print(f"Attempting to backfill data from {start_date_str} to {end_date_str}.")

        # 2. 获取 AKShare 的交易日历，找出这个范围内的所有真实交易日
        tool_trade_date_hist_df = ak.tool_trade_date_hist_df()
        trade_dates = [d.strftime('%Y%m%d') for d in pd.to_datetime(tool_trade_date_hist_df['trade_date'])]
        dates_to_fetch = [d for d in trade_dates if start_date_str <= d <= end_date_str]
        
        if not dates_to_fetch:
            print("No new trading days to fetch in the specified range. Job finished.")
            return
        
        print(f"Found {len(dates_to_fetch)} new trading days to update: {', '.join(dates_to_fetch)}")

        # 3. 从 stocks_info 获取所有有效的 symbol，作为我们的“蓝图”
        print("Fetching valid symbols from Supabase...")
        response = supabase.table('stocks_info').select('symbol').execute()
        valid_symbols = {item['symbol'] for item in response.data}
        print(f"Found {len(valid_symbols)} valid symbols to track.")

        # 4. 逐个交易日进行数据获取和上传
        total_upserted_count = 0
        for trade_date in dates_to_fetch:
            print(f"\n--- Processing date: {trade_date} ---")
            try:
                stock_df = ak.stock_zh_a_hist(symbol="all", period="daily", start_date=trade_date, end_date=trade_date, adjust="")
                if stock_df is None or stock_df.empty:
                    print(f"  -> No data returned from AKShare for {trade_date}.")
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
                            "symbol": symbol, "date": row['date'],
                            "open": row['open'], "high": row['high'],
                            "low": row['low'], "close": row['close'],
                            "volume": int(row['volume']), "amount": int(row['amount']),
                        })
                
                if records_to_upsert:
                    print(f"  -> Upserting {len(records_to_upsert)} valid records for {trade_date}...")
                    supabase.table('daily_bars').upsert(records_to_upsert, on_conflict='symbol,date').execute()
                    total_upserted_count += len(records_to_upsert)
            
            except Exception as e:
                print(f"  -> An error occurred while processing {trade_date}: {e}")

    except Exception as e:
        print(f"An unhandled error occurred in background job: {e}")
        sys.exit(1)
    finally:
        print(f"\n--- Data update job FINISHED. Total new records upserted: {total_upserted_count} ---")

if __name__ == '__main__':
    do_update_job()
