#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
市场聚合数据生成脚本 v1.0

功能：
1. 每日市场统计（涨跌家数、市场平均涨幅等）
2. 板块轮动分析（各板块表现对比）
3. 历史排名追踪（RS Rating、涨幅等排名变化）

用途：
- 市场情绪判断（择时）
- 板块轮动策略
- 龙头股识别
- 历史表现对比

数据输出：
data/market_aggregates/
  ├── daily_stats.parquet       # 每日市场统计
  ├── sector_rotation.parquet   # 板块轮动（待实现）
  └── ranking_history.parquet   # 排名历史

作者：Claude
版本：v1.0
日期：2025-11-03
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from tqdm import tqdm
import logging
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# 配置
# ============================================================

# 目录配置
SNAPSHOT_DIR = Path("data/daily_snapshot")
AGGREGATES_DIR = Path("data/market_aggregates")
LOG_DIR = Path("logs")

# 创建目录
AGGREGATES_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(
            LOG_DIR / f'market_aggregates_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
            encoding='utf-8'
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================================
# 1. 每日市场统计
# ============================================================

def generate_daily_stats(snapshot_date=None):
    """
    生成每日市场统计
    
    统计指标：
    - 涨跌家数统计
    - 涨跌停统计
    - 市场平均涨跌幅
    - 成交量和换手率统计
    - RS Rating分布
    """
    
    print("\n" + "=" * 80)
    print("  生成每日市场统计")
    print("=" * 80)
    
    # 加载快照
    snapshot_file = SNAPSHOT_DIR / "latest.parquet"
    if not snapshot_file.exists():
        if snapshot_date:
            snapshot_file = SNAPSHOT_DIR / f"snapshot_{snapshot_date}.parquet"
        
        if not snapshot_file.exists():
            print(f"❌ 未找到快照文件: {snapshot_file}")
            return None
    
    print(f"读取快照: {snapshot_file.name}")
    df = pd.read_parquet(snapshot_file)
    
    if df.empty:
        print("❌ 快照数据为空")
        return None
    
    # 标准化列名
    rename_map = {
        '日期': 'date',
        '股票代码': 'symbol',
        '涨跌幅': 'pct_change',
        '成交量': 'volume',
        '成交额': 'amount',
        '换手率': 'turnover',
    }
    df = df.rename(columns=rename_map)
    
    # 获取日期
    if 'date' in df.columns:
        stat_date = df['date'].iloc[0]
        if isinstance(stat_date, str):
            stat_date = stat_date
        else:
            stat_date = stat_date.strftime('%Y-%m-%d')
    else:
        stat_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"统计日期: {stat_date}")
    print(f"股票数量: {len(df)}")
    
    # 开始统计
    stats = {'日期': stat_date}
    
    # 1. 涨跌家数统计
    if 'pct_change' in df.columns:
        df['pct_change'] = pd.to_numeric(df['pct_change'], errors='coerce')
        
        stats['上涨家数'] = (df['pct_change'] > 0).sum()
        stats['下跌家数'] = (df['pct_change'] < 0).sum()
        stats['平盘家数'] = (df['pct_change'] == 0).sum()
        
        # 涨跌停（简化判断：涨幅≥9.9%为涨停，≤-9.9%为跌停）
        stats['涨停家数'] = (df['pct_change'] >= 9.9).sum()
        stats['跌停家数'] = (df['pct_change'] <= -9.9).sum()
        
        # 大涨大跌（涨跌幅≥5%）
        stats['大涨家数_涨幅5%'] = (df['pct_change'] >= 5).sum()
        stats['大跌家数_跌幅5%'] = (df['pct_change'] <= -5).sum()
        
        # 市场平均涨跌幅
        stats['市场平均涨跌幅_%'] = df['pct_change'].mean()
        stats['市场涨跌幅中位数_%'] = df['pct_change'].median()
        
        print(f"\n📊 涨跌统计:")
        print(f"  上涨: {stats['上涨家数']} 只 ({stats['上涨家数']/len(df)*100:.1f}%)")
        print(f"  下跌: {stats['下跌家数']} 只 ({stats['下跌家数']/len(df)*100:.1f}%)")
        print(f"  平盘: {stats['平盘家数']} 只")
        print(f"  涨停: {stats['涨停家数']} 只")
        print(f"  跌停: {stats['跌停家数']} 只")
    
    # 2. 成交量和成交额统计
    if 'volume' in df.columns:
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        stats['成交量总和_亿股'] = df['volume'].sum() / 100000000
        stats['成交量平均_万股'] = df['volume'].mean() / 10000
        stats['成交量中位数_万股'] = df['volume'].median() / 10000
        
        print(f"\n📈 成交量:")
        print(f"  总和: {stats['成交量总和_亿股']:.2f} 亿股")
        print(f"  平均: {stats['成交量平均_万股']:.2f} 万股")
    
    if 'amount' in df.columns:
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        stats['成交额总和_亿元'] = df['amount'].sum() / 100000000
        stats['成交额平均_亿元'] = df['amount'].mean() / 100000000
        stats['成交额中位数_亿元'] = df['amount'].median() / 100000000
        
        print(f"\n💰 成交额:")
        print(f"  总和: {stats['成交额总和_亿元']:.2f} 亿元")
        print(f"  平均: {stats['成交额平均_亿元']:.2f} 亿元")
    
    # 3. 换手率统计
    if 'turnover' in df.columns:
        df['turnover'] = pd.to_numeric(df['turnover'], errors='coerce')
        stats['换手率平均_%'] = df['turnover'].mean()
        stats['换手率中位数_%'] = df['turnover'].median()
        
        # 高换手率股票数量（换手率>5%）
        stats['高换手率家数_5%'] = (df['turnover'] > 5).sum()
        
        print(f"\n🔄 换手率:")
        print(f"  平均: {stats['换手率平均_%']:.2f}%")
        print(f"  中位数: {stats['换手率中位数_%']:.2f}%")
    
    # 4. RS Rating分布
    if 'rs_rating' in df.columns:
        df['rs_rating'] = pd.to_numeric(df['rs_rating'], errors='coerce')
        
        stats['RS_Rating平均'] = df['rs_rating'].mean()
        stats['RS_Rating中位数'] = df['rs_rating'].median()
        
        # RS Rating分布
        stats['RS≥90_数量'] = (df['rs_rating'] >= 90).sum()
        stats['RS_80-90_数量'] = ((df['rs_rating'] >= 80) & (df['rs_rating'] < 90)).sum()
        stats['RS_70-80_数量'] = ((df['rs_rating'] >= 70) & (df['rs_rating'] < 80)).sum()
        stats['RS_60-70_数量'] = ((df['rs_rating'] >= 60) & (df['rs_rating'] < 70)).sum()
        stats['RS<60_数量'] = (df['rs_rating'] < 60).sum()
        
        print(f"\n⭐ RS Rating分布:")
        print(f"  ≥90: {stats['RS≥90_数量']} 只")
        print(f"  80-90: {stats['RS_80-90_数量']} 只")
        print(f"  70-80: {stats['RS_70-80_数量']} 只")
        print(f"  60-70: {stats['RS_60-70_数量']} 只")
        print(f"  <60: {stats['RS<60_数量']} 只")
    
    # 5. 价格区间统计
    for ma in ['ma5', 'ma10', 'ma20', 'ma50', 'ma60', 'ma120']:
        if ma in df.columns and '收盘' in df.columns:
            close_col = '收盘' if '收盘' in df.columns else 'close'
            df[ma] = pd.to_numeric(df[ma], errors='coerce')
            df[close_col] = pd.to_numeric(df[close_col], errors='coerce')
            
            above_ma = (df[close_col] > df[ma]).sum()
            stats[f'站上{ma.upper()}_数量'] = above_ma
            stats[f'站上{ma.upper()}_占比_%'] = above_ma / len(df) * 100
    
    # 转换为DataFrame
    df_stats = pd.DataFrame([stats])
    
    return df_stats


def save_daily_stats(df_new_stats):
    """保存或追加每日市场统计"""
    
    stats_file = AGGREGATES_DIR / "daily_stats.parquet"
    
    if stats_file.exists():
        # 读取已有数据
        df_existing = pd.read_parquet(stats_file)
        
        # 检查是否已有今天的数据
        new_date = df_new_stats['日期'].iloc[0]
        
        if new_date in df_existing['日期'].values:
            # 删除旧数据
            df_existing = df_existing[df_existing['日期'] != new_date]
            logger.info(f"更新已有日期的数据: {new_date}")
        
        # 追加新数据
        df_combined = pd.concat([df_existing, df_new_stats], ignore_index=True)
        
        # 按日期排序
        df_combined = df_combined.sort_values('日期')
        
    else:
        df_combined = df_new_stats
    
    # 保存
    df_combined.to_parquet(stats_file, index=False)
    logger.info(f"✅ 市场统计已保存: {stats_file}")
    logger.info(f"  总记录数: {len(df_combined)}")
    
    return stats_file


# ============================================================
# 2. 排名历史追踪
# ============================================================

def generate_ranking_history(snapshot_date=None):
    """
    生成排名历史
    
    追踪指标：
    - RS Rating排名
    - 涨幅排名
    - 成交量排名
    """
    
    print("\n" + "=" * 80)
    print("  生成排名历史")
    print("=" * 80)
    
    # 加载快照
    snapshot_file = SNAPSHOT_DIR / "latest.parquet"
    if not snapshot_file.exists():
        if snapshot_date:
            snapshot_file = SNAPSHOT_DIR / f"snapshot_{snapshot_date}.parquet"
        
        if not snapshot_file.exists():
            print(f"❌ 未找到快照文件: {snapshot_file}")
            return None
    
    print(f"读取快照: {snapshot_file.name}")
    df = pd.read_parquet(snapshot_file)
    
    if df.empty:
        print("❌ 快照数据为空")
        return None
    
    # 标准化列名
    rename_map = {
        '日期': 'date',
        '股票代码': 'symbol',
        '股票名称': 'name',
        '涨跌幅': 'pct_change',
        '成交量': 'volume',
    }
    df = df.rename(columns=rename_map)
    
    # 获取日期
    if 'date' in df.columns:
        rank_date = df['date'].iloc[0]
        if isinstance(rank_date, str):
            rank_date = rank_date
        else:
            rank_date = rank_date.strftime('%Y-%m-%d')
    else:
        rank_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"排名日期: {rank_date}")
    
    # 计算排名
    rankings = []
    
    for _, row in df.iterrows():
        rank_data = {
            '日期': rank_date,
            'symbol': row.get('symbol', ''),
            'name': row.get('name', ''),
        }
        
        # RS Rating排名
        if 'rs_rating' in df.columns:
            rank_data['rs_rating'] = row.get('rs_rating', np.nan)
            rank_data['rs_rating_rank'] = (df['rs_rating'] >= row.get('rs_rating', 0)).sum()
        
        # 涨幅排名
        if 'pct_change' in df.columns:
            rank_data['pct_change'] = row.get('pct_change', np.nan)
            rank_data['pct_change_rank'] = (df['pct_change'] >= row.get('pct_change', 0)).sum()
        
        # 成交量排名
        if 'volume' in df.columns:
            rank_data['volume'] = row.get('volume', np.nan)
            rank_data['volume_rank'] = (df['volume'] >= row.get('volume', 0)).sum()
        
        rankings.append(rank_data)
    
    df_rankings = pd.DataFrame(rankings)
    
    print(f"✓ 生成排名数据: {len(df_rankings)} 条")
    
    return df_rankings


def save_ranking_history(df_new_rankings):
    """保存或追加排名历史"""
    
    ranking_file = AGGREGATES_DIR / "ranking_history.parquet"
    
    if ranking_file.exists():
        # 读取已有数据
        df_existing = pd.read_parquet(ranking_file)
        
        # 检查是否已有今天的数据
        new_date = df_new_rankings['日期'].iloc[0]
        
        if new_date in df_existing['日期'].values:
            # 删除旧数据
            df_existing = df_existing[df_existing['日期'] != new_date]
            logger.info(f"更新已有日期的数据: {new_date}")
        
        # 追加新数据
        df_combined = pd.concat([df_existing, df_new_rankings], ignore_index=True)
        
        # 按日期排序
        df_combined = df_combined.sort_values(['日期', 'symbol'])
        
    else:
        df_combined = df_new_rankings
    
    # 保存
    df_combined.to_parquet(ranking_file, index=False)
    logger.info(f"✅ 排名历史已保存: {ranking_file}")
    logger.info(f"  总记录数: {len(df_combined)}")
    
    return ranking_file


# ============================================================
# 3. 板块轮动分析（待实现）
# ============================================================

def generate_sector_rotation():
    """
    生成板块轮动数据
    
    TODO: 需要板块分类数据
    - 板块平均涨跌幅
    - 板块资金流入
    - 板块相对强度
    """
    print("\n⚠️  板块轮动分析功能待实现")
    print("   需要先建立股票-板块映射关系")
    return None


# ============================================================
# 主程序
# ============================================================

def main():
    print("=" * 80)
    print("  市场聚合数据生成器 v1.0")
    print("  - 每日市场统计")
    print("  - 排名历史追踪")
    print("=" * 80)
    
    # 检查快照目录
    if not SNAPSHOT_DIR.exists():
        print(f"\n❌ 快照目录不存在: {SNAPSHOT_DIR}")
        print(f"💡 请先运行 generate_daily_snapshot.py 生成快照")
        return
    
    # 1. 生成每日市场统计
    print("\n" + "=" * 80)
    print("任务1: 每日市场统计")
    print("=" * 80)
    
    df_stats = generate_daily_stats()
    
    if df_stats is not None:
        stats_file = save_daily_stats(df_stats)
    else:
        print("❌ 市场统计生成失败")
        stats_file = None
    
    # 2. 生成排名历史
    print("\n" + "=" * 80)
    print("任务2: 排名历史追踪")
    print("=" * 80)
    
    df_rankings = generate_ranking_history()
    
    if df_rankings is not None:
        ranking_file = save_ranking_history(df_rankings)
    else:
        print("❌ 排名历史生成失败")
        ranking_file = None
    
    # 3. 板块轮动（待实现）
    # generate_sector_rotation()
    
    # 完成
    print("\n" + "=" * 80)
    print("✅ 市场聚合数据生成完成！")
    print("=" * 80)
    
    if stats_file:
        print(f"\n📊 市场统计: {stats_file}")
    if ranking_file:
        print(f"📊 排名历史: {ranking_file}")
    
    print(f"\n💡 数据用途:")
    print(f"  - 市场情绪判断（择时参考）")
    print(f"  - 追踪个股排名变化")
    print(f"  - 分析市场整体趋势")
    
    print(f"\n📈 后续优化:")
    print(f"  - 添加板块轮动分析")
    print(f"  - 添加资金流向统计")
    print(f"  - 添加市场情绪指标")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
