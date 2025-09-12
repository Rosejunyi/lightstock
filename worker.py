# worker.py (只替换这个函数)

def calculate_and_update_indicators(supabase: Client, target_date: datetime.date):
    """ 在 Python 端计算并更新指定日期的技术指标 (修正版) """
    print("\n--- Step 3: Calculating Technical Indicators ---")
    target_date_str = target_date.strftime('%Y-%m-%d')
    try:
        print("Fetching recent historical data for indicator calculation...")
        response = supabase.table('daily_bars') \
            .select('symbol, date, close') \
            .gte('date', (target_date - timedelta(days=90)).strftime('%Y-%m-%d')) \
            .lte('date', target_date_str) \
            .order('date', desc=False) \
            .execute()

        if not response.data:
            print("  -> No historical data found to calculate indicators. Skipping."); return

        df = pd.DataFrame(response.data)
        print(f"  -> Fetched {len(df)} rows for calculation.")
        
        # --- 关键修复：使用 groupby().apply() 来为每个分组独立计算 ---
        def calculate_ta(group):
            # pandas_ta 会直接在传入的 group 上增加新的列
            group.ta.rsi(length=14, append=True)
            group.ta.sma(length=5, append=True)
            group.ta.sma(length=10, append=True)
            return group

        # 对每个 symbol 分组，然后应用我们的计算函数
        df_with_ta = df.groupby('symbol', group_keys=False).apply(calculate_ta)
        # -------------------------------------------------------------
        
        # 筛选出目标日期当天的、计算好的指标
        today_indicators = df_with_ta[df_with_ta['date'] == target_date_str].copy()
        
        records_to_upsert = []
        # pandas-ta 生成的列名是 "RSI_14", "SMA_5", "SMA_10"
        for index, row in today_indicators.iterrows():
            if pd.notna(row['SMA_5']) and pd.notna(row['SMA_10']) and pd.notna(row['RSI_14']):
                records_to_upsert.append({
                    'symbol': row['symbol'],
                    'date': row['date'],
                    'ma5': row['SMA_5'],
                    'ma10': row['SMA_10'],
                    'rsi14': row['RSI_14']
                })
        
        if records_to_upsert:
            print(f"  -> Calculated indicators for {len(records_to_upsert)} stocks. Upserting to daily_metrics...")
            supabase.table('daily_metrics').upsert(records_to_upsert, on_conflict='symbol,date').execute()
            print("  -> Technical indicators updated successfully!")
        else:
            print("  -> No valid indicators calculated for today.")

    except Exception as e:
        print(f"  -> An error occurred during indicator calculation: {e}")
