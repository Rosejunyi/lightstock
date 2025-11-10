#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成每日市场快照 - 用于快速全市场筛选

功能：
1. 从所有个股指标文件中提取最新一天的数据
2. 合并成单个parquet文件
3. 便于快速筛选和排序

作者：Claude
版本：v1.0
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import logging

# 配置
INDICATORS_DIR = Path("data/technical_indicators")
SNAPSHOT_DIR = Path("data/daily_snapshot")
LOG_DIR = Path("logs")

# 创建目录
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(
            LOG_DIR / f'generate_snapshot_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
            encoding='utf-8'
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def extract_latest_data(file_path):
    """从单个文件中提取最新一天的数据"""
    try:
        df = pd.read_parquet(file_path)
        
        if df.empty:
            return None
        
        # 只取最后一行（最新一天）
        latest = df.iloc[-1:].copy()
        
        # 添加股票代码（如果没有）
        if '股票代码' not in latest.columns:
            latest['股票代码'] = file_path.stem
        
        return latest
        
    except Exception as e:
        logger.error(f"{file_path.name}: 读取失败 - {e}")
        return None


def generate_daily_snapshot():
    """生成每日市场快照"""
    
    print("=" * 80)
    print("  生成每日市场快照")
    print("=" * 80)
    
    # 获取所有指标文件
    indicator_files = list(INDICATORS_DIR.glob("*.parquet"))
    
    if not indicator_files:
        print(f"❌ 未找到指标文件: {INDICATORS_DIR}")
        return False
    
    print(f"\n找到 {len(indicator_files)} 个股票指标文件")
    
    # 提取最新数据
    print(f"\n📖 提取最新数据...")
    
    all_latest = []
    failed_count = 0
    
    for file in tqdm(indicator_files, desc="提取进度"):
        latest = extract_latest_data(file)
        if latest is not None:
            all_latest.append(latest)
        else:
            failed_count += 1
    
    if not all_latest:
        print("❌ 没有成功提取任何数据")
        return False
    
    print(f"✅ 成功提取 {len(all_latest)} 只股票的最新数据")
    if failed_count > 0:
        print(f"⚠️  提取失败 {failed_count} 只股票")
    
    # 合并数据
    print(f"\n🔗 合并数据...")
    df_snapshot = pd.concat(all_latest, ignore_index=True)
    
    # 获取日期（用于文件名）
    snapshot_date = df_snapshot['日期'].iloc[0]
    print(f"  快照日期: {snapshot_date}")
    print(f"  股票数量: {len(df_snapshot):,}")
    print(f"  字段数量: {len(df_snapshot.columns)}")
    
    # 排序（按RS Rating降序）
    if 'rs_rating' in df_snapshot.columns:
        df_snapshot = df_snapshot.sort_values('rs_rating', ascending=False)
        print(f"  ✅ 已按RS Rating排序")
    
    # 保存快照
    print(f"\n💾 保存快照...")
    
    # 1. 保存为latest.parquet（覆盖）
    latest_file = SNAPSHOT_DIR / "latest.parquet"
    df_snapshot.to_parquet(latest_file, index=False)
    print(f"  ✅ 已保存: {latest_file}")
    
    # 2. 保存为带日期的文件（归档）
    dated_file = SNAPSHOT_DIR / f"snapshot_{snapshot_date}.parquet"
    df_snapshot.to_parquet(dated_file, index=False)
    print(f"  ✅ 已保存: {dated_file}")
    
    # 显示文件大小
    latest_size = latest_file.stat().st_size / 1024 / 1024
    print(f"\n📊 快照文件大小: {latest_size:.2f} MB")
    
    # 显示示例数据
    print(f"\n📈 强势股TOP 10:")
    print(df_snapshot[['股票代码', '股票名称', 'rs_rating', 'rsi14', 'ma20', '收盘']].head(10).to_string(index=False))
    
    print("\n" + "=" * 80)
    print("✅ 市场快照生成完成！")
    print("=" * 80)
    print(f"💾 快照位置: {SNAPSHOT_DIR}")
    print(f"📄 最新快照: latest.parquet")
    print(f"📄 归档快照: snapshot_{snapshot_date}.parquet")
    
    return True


if __name__ == "__main__":
    success = generate_daily_snapshot()
    exit(0 if success else 1)
