#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GitHub Actions 清洗脚本
清洗 adjusted_parquet 数据，保留所有列
"""

import pandas as pd
from pathlib import Path
import logging
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def clean_stock_data(input_file, output_file):
    """清洗单个文件，保留所有列"""
    try:
        df = pd.read_parquet(input_file)
        
        # 确保日期格式并设为索引
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
        elif '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'])
            df.set_index('日期', inplace=True)
        else:
            logging.warning(f'{input_file.name}: 没有日期列')
            return False
        
        # 排序、去重、删除缺失值
        df.sort_index(inplace=True)
        df = df[~df.index.duplicated(keep='last')]
        df.dropna(inplace=True)
        
        if len(df) > 0:
            df.to_parquet(output_file, compression='snappy')
            return True
        return False
        
    except Exception as e:
        logging.error(f'{input_file.name}: {e}')
        return False

def main():
    print("="*50)
    print("  清洗复权数据（保留所有列）")
    print("="*50)
    print()
    
    # 路径
    base_dir = Path(__file__).parent.parent
    adjusted_dir = base_dir / 'data' / 'adjusted_parquet'
    cleaned_dir = base_dir / 'data' / 'cleaned_parquet'
    cleaned_dir.mkdir(exist_ok=True, parents=True)
    
    if not adjusted_dir.exists():
        print(f"❌ 目录不存在: {adjusted_dir}")
        return
    
    # 获取所有文件
    files = list(adjusted_dir.glob('*.parquet'))
    print(f"📁 找到 {len(files)} 个文件")
    print()
    
    success = 0
    failed = 0
    skipped = 0
    
    for input_file in tqdm(files, desc="清洗进度"):
        output_file = cleaned_dir / input_file.name
        
        # 跳过已存在且较新的文件
        if output_file.exists():
            if output_file.stat().st_mtime > input_file.stat().st_mtime:
                skipped += 1
                continue
        
        if clean_stock_data(input_file, output_file):
            success += 1
        else:
            failed += 1
    
    print()
    print("="*50)
    print(f"✅ 成功: {success}")
    print(f"⏭️  跳过: {skipped}")
    print(f"❌ 失败: {failed}")
    
    # 统计结果
    cleaned_files = list(cleaned_dir.glob('*.parquet'))
    if cleaned_files:
        total_size = sum(f.stat().st_size for f in cleaned_files) / 1024 / 1024
        print(f"📊 总计: {len(cleaned_files)} 个文件, {total_size:.2f} MB")
        
        # 显示示例
        sample = cleaned_files[0]
        sample_df = pd.read_parquet(sample)
        print(f"\n📋 示例文件: {sample.name}")
        print(f"   列名: {list(sample_df.columns)}")
        print(f"   数据量: {len(sample_df)} 行")
    
    print("="*50)

if __name__ == '__main__':
    main()
