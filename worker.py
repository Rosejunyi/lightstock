# worker.py (只替换这个函数)
def calculate_and_update_indicators_mytt(supabase: Client, target_date: datetime.date):
    """ 使用 MyTT 在 Python 端计算并更新【所有】技术和衍生指标 """
    print("\n--- Step 3: Calculating ALL Technical & Derived Indicators using MyTT ---")
    target_date_str = target_date.strftime('%Y-%m-%d')
    try:
        print("Fetching recent historical data for calculation...")
        
        # 1. 获取计算所需的所有历史数据（我们需要更多天数来计算 MA60）
        all_historical_data = []
        page = 0
        while True:
            # 为了计算 MA60 和更长的指标，我们需要获取更多历史数据，比如 150 天
            response = supabase.table('daily_bars') \
                .select('symbol, date, open, high, low, close, volume') \
                .gte('date', (target_date - timedelta(days=150)).strftime('%Y-%m-%d')) \
                .lte('date', target_date_str) \
                .order('date', desc=False) \
                .range(page * 1000, (page + 1) * 1000 - 1).execute()
            if not response.data: break
            all_historical_data.extend(response.data)
            if len(response.data) < 1000: break
            page += 1
        
        if not all_historical_data:
            print("  -> No historical data found. Skipping."); return

        df = pd.DataFrame(all_historical_data)
        print(f"  -> Fetched {len(df)} rows for calculation.")
        
        # --- 2. 核心计算逻辑：使用 MyTT 进行分组计算 ---
        def calculate_all_mytt(group):
            # MyTT 需要的输入是 Numpy 数组
            CLOSE = group['close'].values
            HIGH = group['high'].values
            LOW = group['low'].values
            OPEN = group['open'].values
            VOL = group['volume'].values.astype(float) # 成交量需要是浮点数
            
            # 安全检查：确保有足够的数据
            if len(CLOSE) < 60: return group

            # a. 计算涨跌幅
            group['change_percent'] = (CLOSE / REF(CLOSE, 1) - 1) * 100
            
            # b. 计算均量比
            VOL_MA5 = MA(VOL, 5)
            # 避免除以0的错误
            group['volume_ratio_5d'] = np.where(VOL_MA5 > 0, VOL / VOL_MA5, 0)
            
            # c. 计算10日涨跌幅
            group['change_percent_10d'] = (CLOSE / REF(CLOSE, 10) - 1) * 100
            
            # d. 计算所有均线
            group['ma5'] = MA(CLOSE, 5)
            group['ma10'] = MA(CLOSE, 10)
            group['ma20'] = MA(CLOSE, 20)
            group['ma60'] = MA(CLOSE, 60)
            
            # e. 计算 MACD
            DIF, DEA, MACD_BAR = MACD(CLOSE)
            group['macd_diff'] = DIF
            group['macd_dea'] = DEA
            
            # f. 计算 KDJ
            K, D, J = KDJ(CLOSE, HIGH, LOW)
            group['kdj_k'] = K
            group['kdj_d'] = D
            group['kdj_j'] = J
            
            # g. 计算 RSI
            group['rsi14'] = RSI(CLOSE, 14)
            
            return group

        print("  -> Calculating all indicators for all symbols...")
        df_with_ta = df.groupby('symbol', group_keys=False).apply(calculate_all_mytt)
        print("  -> Calculation finished.")
        
        # 3. 筛选出目标日期当天的、计算好的指标
        today_indicators = df_with_ta[df_with_ta['date'] == target_date_str].copy()
        
        # 4. 准备上传数据
        records_to_upsert = []
        # 我们需要更新的所有字段
        indicator_columns = [
            'change_percent', 'volume_ratio_5d', 'change_percent_10d',
            'ma5', 'ma10', 'ma20', 'ma60',
            'macd_diff', 'macd_dea',
            'kdj_k', 'kdj_d', 'kdj_j',
            'rsi14'
        ]
        
        for index, row in today_indicators.iterrows():
            record = {'symbol': row['symbol'], 'date': row['date']}
            is_valid = True
            for col in indicator_columns:
                if col in row and pd.notna(row[col]):
                    record[col] = float(row[col])
                else:
                    # 如果任何一个核心指标为空，我们可能不希望写入
                    # is_valid = False
                    # break
                    record[col] = None # 或者我们允许部分指标为空
            
            # if is_valid:
            records_to_upsert.append(record)
        
        # 5. 将计算结果 upsert 回 daily_metrics 表
        if records_to_upsert:
            print(f"  -> Found {len(records_to_upsert)} stocks with valid indicators. Upserting...")
            supabase.table('daily_metrics').upsert(records_to_upsert, on_conflict='symbol,date').execute()
            print("  -> All technical and derived indicators updated successfully!")

    except Exception as e:
        print(f"  -> An error occurred during MyTT calculation: {e}")
