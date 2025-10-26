#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
复权数据处理脚本 V2 - 适配GitHub Actions
支持增量更新和智能修复
"""

import baostock as bs
import pandas as pd
import os
from tqdm import tqdm
from datetime import datetime, timedelta
from pathlib import Path

print("="*60)
print("  股票复权数据处理 V2 (增量更新模式)")
print("="*60)
print()

# --- 路径定义 ---
BASE_DIR = Path(__file__).parent.parent
RAW_DATA_PATH = BASE_DIR / "data" / "daily_parquet"
PROCESSED_DATA_PATH = BASE_DIR / "data" / "adjusted_parquet"

# 确保目录存在
PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)

# 获取股票文件列表（排除指数）
try:
    stock_files = sorted([
        f.stem for f in RAW_DATA_PATH.glob("*.parquet")
        if not (f.name.startswith('sh.') or f.name.startswith('sz.'))
    ])
    print(f"📁 找到 {len(stock_files)} 个股票文件")
except Exception as e:
    print(f"❌ 读取原始数据目录失败: {e}")
    stock_files = []

# 指数列表（不需要复权，直接复制）
INDEX_LIST = ["sh.000300", "sh.000001", "sz.399001", "sz.399006"]

# --- 登录Baostock ---
print("\n🔐 登录Baostock...")
lg = bs.login()
if lg.error_code != '0':
    print(f"❌ Baostock登录失败: {lg.error_msg}")
    exit(1)
print("✅ 登录成功")
print()


def process_stock_with_adjustment(code, raw_file_path, processed_file_path):
    """
    处理单个股票的复权数据（增量更新）
    """
    if not raw_file_path.exists():
        return
    
    # 确定起始日期
    start_date = '1990-01-01'
    existing_df = None
    
    # 检查是否已有处理过的数据
    if processed_file_path.exists():
        try:
            existing_df = pd.read_parquet(processed_file_path)
            if not existing_df.empty:
                last_date = pd.to_datetime(existing_df['日期'].iloc[-1])
                # 从最后一天的下一天开始更新
                start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
                
                # 如果起始日期已经超过今天，跳过
                if start_date > datetime.now().strftime('%Y-%m-%d'):
                    return
        except Exception as e:
            tqdm.write(f"⚠️ 读取已处理文件失败，将全新处理: {e}")
            existing_df = None
    
    try:
        # 读取原始数据
        df_raw = pd.read_parquet(raw_file_path)
        df_raw['日期'] = pd.to_datetime(df_raw['日期'])
        
        # 筛选需要处理的新数据
        new_raw_df = df_raw[df_raw['日期'] >= pd.to_datetime(start_date)].copy()
        
        if new_raw_df.empty:
            return
        
        new_raw_df.set_index('日期', inplace=True)
        
        # 构建baostock代码格式
        code_bs = ("sh." if code.startswith('6') else "sz.") + code
        
        # 获取前复权数据
        def get_adjust_data(flag):
            data_list = []
            rs = bs.query_history_k_data_plus(
                code_bs,
                "date,open,high,low,close",
                start_date=start_date,
                frequency="d",
                adjustflag=flag
            )
            while (rs.error_code == '0') and rs.next():
                data_list.append(rs.get_row_data())
            return pd.DataFrame(data_list, columns=rs.fields) if data_list else pd.DataFrame()
        
        # 获取前复权（adjustflag="2"）
        df_qfq = get_adjust_data("2")
        if not df_qfq.empty:
            df_qfq.rename(columns={
                'date': '日期',
                'open': 'qfq_开盘',
                'high': 'qfq_最高',
                'low': 'qfq_最低',
                'close': 'qfq_收盘'
            }, inplace=True)
            df_qfq['日期'] = pd.to_datetime(df_qfq['日期'])
            df_qfq.set_index('日期', inplace=True)
            for col in ['qfq_开盘', 'qfq_最高', 'qfq_最低', 'qfq_收盘']:
                df_qfq[col] = pd.to_numeric(df_qfq[col], errors='coerce')
        
        # 获取后复权（adjustflag="1"）
        df_hfq = get_adjust_data("1")
        if not df_hfq.empty:
            df_hfq.rename(columns={
                'date': '日期',
                'open': 'hfq_开盘',
                'high': 'hfq_最高',
                'low': 'hfq_最低',
                'close': 'hfq_收盘'
            }, inplace=True)
            df_hfq['日期'] = pd.to_datetime(df_hfq['日期'])
            df_hfq.set_index('日期', inplace=True)
            for col in ['hfq_开盘', 'hfq_最高', 'hfq_最低', 'hfq_收盘']:
                df_hfq[col] = pd.to_numeric(df_hfq[col], errors='coerce')
        
        # 合并原始数据和复权数据
        newly_processed_df = pd.concat([new_raw_df, df_qfq, df_hfq], axis=1)
        newly_processed_df.reset_index(inplace=True)
        
        # 如果存在旧数据，进行合并
        if existing_df is not None:
            final_df = pd.concat([existing_df, newly_processed_df], ignore_index=True)
            final_df.drop_duplicates(subset=['日期'], keep='last', inplace=True)
            tqdm.write(f"  ✅ {code}: 增量更新 {len(newly_processed_df)} 条数据")
        else:
            final_df = newly_processed_df
            tqdm.write(f"  ✅ {code}: 全新处理 {len(final_df)} 条数据")
        
        # 保存
        final_df.to_parquet(processed_file_path, index=False)
        
    except Exception as e:
        tqdm.write(f"  ❌ {code} 处理失败: {e}")


def copy_index_file(index_code, raw_path, processed_path):
    """
    复制指数文件（指数不需要复权）
    """
    raw_file = raw_path / f"{index_code}.parquet"
    processed_file = processed_path / f"{index_code}.parquet"
    
    if not raw_file.exists():
        tqdm.write(f"  ⚠️ {index_code}: 原始文件不存在")
        return
    
    try:
        # 直接复制
        df = pd.read_parquet(raw_file)
        df.to_parquet(processed_file, index=False)
        tqdm.write(f"  ✅ {index_code}: 复制完成（指数无需复权）")
    except Exception as e:
        tqdm.write(f"  ❌ {index_code}: 复制失败 - {e}")


# --- 主流程 ---
if stock_files:
    print("="*60)
    print("📊 开始处理股票复权数据")
    print("="*60)
    print()
    
    for stock_code in tqdm(stock_files, desc="处理股票"):
        raw_file = RAW_DATA_PATH / f"{stock_code}.parquet"
        processed_file = PROCESSED_DATA_PATH / f"{stock_code}.parquet"
        process_stock_with_adjustment(stock_code, raw_file, processed_file)

print()
print("="*60)
print("📈 开始处理指数数据")
print("="*60)
print()

for index_code in tqdm(INDEX_LIST, desc="处理指数"):
    copy_index_file(index_code, RAW_DATA_PATH, PROCESSED_DATA_PATH)

# --- 登出 ---
bs.logout()

print()
print("="*60)
print("📊 统计信息")
print("="*60)

# 统计文件数量
stock_count = len(list(PROCESSED_DATA_PATH.glob("*.parquet"))) - len(INDEX_LIST)
index_count = len([f for f in PROCESSED_DATA_PATH.glob("*.parquet") 
                   if f.stem in INDEX_LIST])

print(f"✅ 股票文件: {stock_count}")
print(f"✅ 指数文件: {index_count}")
print(f"✅ 总文件数: {stock_count + index_count}")
print(f"✅ 输出目录: {PROCESSED_DATA_PATH}")
print()
print("🎉 所有数据处理完成！")
