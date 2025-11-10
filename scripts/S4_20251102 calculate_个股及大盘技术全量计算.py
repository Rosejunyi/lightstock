#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整技术指标计算工具（含大盘数据处理 + RS Rating）- v4.0 增强版

新增功能（v4.0）：
1. 添加 RS Raw 计算（相对强度原始值）
2. 添加 RS Rating 计算（全市场横向百分位排名）
3. 添加 MA50, MA150, MA200 均线
4. RS 指标同时适用于个股和大盘指数

保留功能（v3.0）：
1. 同时处理个股和大盘指数数据
2. 大盘数据也计算完整的技术指标
3. 输出到独立的 index_indicators 目录
4. 保持原有所有功能

数据源：
- 个股：data/daily_parquet_qfq/
- 大盘：data/index_data/

输出：
- 个股指标：data/technical_indicators/
- 大盘指标：data/index_indicators/

作者：Claude
版本：v4.0（增强版：含RS Rating）
日期：2025-11-01
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import logging
import multiprocessing as mp
from functools import partial
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# 配置
# ============================================================

# 目录配置
INPUT_STOCK_DIR = Path("data/daily_parquet_qfq")      # 个股前复权K线数据
INPUT_INDEX_DIR = Path("data/index_data")              # 大盘指数数据
OUTPUT_STOCK_DIR = Path("data/technical_indicators")   # 个股技术指标输出
OUTPUT_INDEX_DIR = Path("data/index_indicators")       # 大盘技术指标输出
LOG_DIR = Path("logs")

# 计算配置
USE_MULTIPROCESSING = True      # 是否使用多进程
NUM_PROCESSES = mp.cpu_count()  # 进程数
BATCH_SIZE = 100                # 批处理大小

# 小数位数配置
DECIMAL_CONFIG = {
    'ma': 2,           # 移动平均线：2位小数
    'rsi': 2,          # RSI：2位小数
    'macd': 4,         # MACD：4位小数
    'kdj': 4,          # KDJ：4位小数
    'change': 2,       # 涨跌幅：2位小数
    'volume': 2,       # 成交量指标：2位小数
    'price': 2,        # 价格区间：2位小数
    'rs_rating': 1,    # RS Rating：1位小数（百分位）
    'rs_raw': 4,       # RS原始值：4位小数
}

# 创建目录
OUTPUT_STOCK_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_INDEX_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(
            LOG_DIR / f'calculate_indicators_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
            encoding='utf-8'
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# 列名标准化
# ============================================================

def normalize_columns(df, is_index=False):
    """
    标准化列名 - 转换为中文
    
    参数：
    - is_index: 是否为指数数据（影响股票代码字段名）
    """
    if is_index:
        rename_map = {
            'index_code': '指数代码',
            'index_name': '指数名称',
            'symbol': '指数代码',
            'name': '指数名称',
            'date': '日期',
            'open': '开盘',
            'high': '最高',
            'low': '最低',
            'close': '收盘',
            'volume': '成交量',
            'amount': '成交额',
            'turnover': '换手率',
            'pct_change': '涨跌幅',
        }
    else:
        rename_map = {
            'symbol': '股票代码',
            'date': '日期',
            'open': '开盘',
            'high': '最高',
            'low': '最低',
            'close': '收盘',
            'volume': '成交量',
            'amount': '成交额',
            'turnover': '换手率',
            'pct_change': '涨跌幅',
            'name': '股票名称',
        }
    
    df = df.rename(columns=rename_map)
    
    # 检查必需字段
    required = ['日期', '开盘', '最高', '最低', '收盘', '成交量']
    for col in required:
        if col not in df.columns:
            raise ValueError(f"缺少必需字段: {col}")
    
    return df

def convert_to_english_for_calculation(df, is_index=False):
    """计算时临时转换为英文列名（便于代码处理）"""
    if is_index:
        rename_map = {
            '指数代码': 'symbol',
            '指数名称': 'name',
            '日期': 'date',
            '开盘': 'open',
            '最高': 'high',
            '最低': 'low',
            '收盘': 'close',
            '成交量': 'volume',
            '成交额': 'amount',
            '换手率': 'turnover',
            '涨跌幅': 'pct_change',
        }
    else:
        rename_map = {
            '股票代码': 'symbol',
            '日期': 'date',
            '开盘': 'open',
            '最高': 'high',
            '最低': 'low',
            '收盘': 'close',
            '成交量': 'volume',
            '成交额': 'amount',
            '换手率': 'turnover',
            '涨跌幅': 'pct_change',
            '股票名称': 'name',
        }
    return df.rename(columns=rename_map)

# ============================================================
# 技术指标计算函数
# ============================================================

def calculate_ma(df, periods=[5, 10, 20, 30, 50, 60, 120, 150, 200, 250]):
    """
    计算移动平均线
    
    新增：ma50, ma150, ma200（v4.0）
    保留：ma5, ma10, ma20, ma30, ma60, ma120, ma250（v3.0）
    """
    for period in periods:
        df[f'ma{period}'] = df['close'].rolling(window=period).mean()
    return df

def calculate_rsi(df, periods=[6, 12, 14, 24]):
    """计算RSI（相对强弱指标）- 多周期"""
    for period in periods:
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        df[f'rsi{period}'] = rsi
    return df

def calculate_macd(df, fast=12, slow=26, signal=9):
    """计算MACD"""
    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
    
    macd = ema_fast - ema_slow
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    histogram = macd - signal_line
    
    df['macd_dif'] = macd
    df['macd_dea'] = signal_line
    df['macd'] = histogram  # 也叫MACD柱
    df['macd_bar'] = histogram
    
    return df

def calculate_kdj(df, n=9, m1=3, m2=3):
    """计算KDJ指标"""
    low_min = df['low'].rolling(window=n).min()
    high_max = df['high'].rolling(window=n).max()
    
    rsv = (df['close'] - low_min) / (high_max - low_min) * 100
    
    k = rsv.ewm(alpha=1/m1, adjust=False).mean()
    d = k.ewm(alpha=1/m2, adjust=False).mean()
    j = 3 * k - 2 * d
    
    df['kdj_k'] = k
    df['kdj_d'] = d
    df['kdj_j'] = j
    
    return df

def calculate_price_changes(df, periods=[1, 5, 10, 20, 25, 30, 60, 120, 180, 250]):
    """计算不同周期的涨跌幅"""
    for period in periods:
        df[f'change_{period}d'] = df['close'].pct_change(period) * 100
    return df

def calculate_volume_indicators(df):
    """计算成交量指标"""
    # 成交量移动平均
    df['volume_ma5'] = df['volume'].rolling(window=5).mean()
    df['volume_ma10'] = df['volume'].rolling(window=10).mean()
    df['volume_ma20'] = df['volume'].rolling(window=20).mean()
    df['volume_ma30'] = df['volume'].rolling(window=30).mean()
    
    # 量比（5日）
    df['volume_ma60'] = df['volume'].rolling(window=60).mean()  # 新增
    df['volume_ma90'] = df['volume'].rolling(window=90).mean()  # 新增
    df['volume_ratio_5d'] = df['volume'] / df['volume'].rolling(window=5).mean()
    
    return df

def calculate_price_range(df):
    """计算价格区间（高低点）"""
    # 20日、60日、120日、250日高低点
    for period in [20, 60, 120, 250]:
        df[f'high_{period}d'] = df['high'].rolling(window=period).max()
        df[f'low_{period}d'] = df['low'].rolling(window=period).min()
    
    # 52周高低点（约252个交易日）
    df['high_52w'] = df['high'].rolling(window=252).max()
    df['low_52w'] = df['low'].rolling(window=252).min()
    
    return df

def calculate_rs_raw(df):
    """
    计算相对强度原始值 (RS Raw) - v4.0 新增
    
    原理：
    1. 计算不同周期的涨跌幅（20日、60日、120日、250日）
    2. 加权平均（近期权重更高）
    3. 得到相对强度原始值
    
    注：
    - 个股和大盘都可以计算此值
    - 排名需要在所有股票计算完后统一处理
    """
    # 计算不同周期的涨跌幅
    periods = [20, 60, 120, 250]  # 1月、3月、6月、1年
    weights = [0.4, 0.3, 0.2, 0.1]  # 权重：近期权重更高
    
    rs_values = []
    for period in periods:
        change = df['close'].pct_change(period)
        rs_values.append(change)
    
    # 加权平均
    rs_raw = sum(w * rs for w, rs in zip(weights, rs_values))
    df['rs_raw'] = rs_raw
    
    return df

def round_indicators(df):
    """控制指标的小数位数"""
    # MA系列
    ma_cols = [col for col in df.columns if col.startswith('ma') and col[2:].isdigit()]
    for col in ma_cols:
        df[col] = df[col].round(DECIMAL_CONFIG['ma'])
    
    # RSI
    rsi_cols = [col for col in df.columns if col.startswith('rsi')]
    for col in rsi_cols:
        df[col] = df[col].round(DECIMAL_CONFIG['rsi'])
    
    # MACD系列
    macd_cols = [col for col in df.columns if 'macd' in col.lower()]
    for col in macd_cols:
        df[col] = df[col].round(DECIMAL_CONFIG['macd'])
    
    # KDJ系列
    kdj_cols = [col for col in df.columns if col.startswith('kdj_')]
    for col in kdj_cols:
        df[col] = df[col].round(DECIMAL_CONFIG['kdj'])
    
    # 涨跌幅系列
    change_cols = [col for col in df.columns if col.startswith('change_')]
    for col in change_cols:
        df[col] = df[col].round(DECIMAL_CONFIG['change'])
    
    # 成交量指标
    volume_cols = [col for col in df.columns if 'volume' in col and col != 'volume' and col != '成交量']
    for col in volume_cols:
        df[col] = df[col].round(DECIMAL_CONFIG['volume'])
    
    # 价格区间
    price_cols = [col for col in df.columns if 'high_' in col or 'low_' in col]
    for col in price_cols:
        df[col] = df[col].round(DECIMAL_CONFIG['price'])
    
    # RS原始值（v4.0新增）
    if 'rs_raw' in df.columns:
        df['rs_raw'] = df['rs_raw'].round(DECIMAL_CONFIG['rs_raw'])
    
    # RS Rating（稍后计算）
    if 'rs_rating' in df.columns:
        df['rs_rating'] = df['rs_rating'].round(DECIMAL_CONFIG['rs_rating'])
    
    return df

def calculate_all_indicators(df, is_index=False):
    """
    计算所有技术指标
    
    参数：
    - df: 原始K线数据
    - is_index: 是否为指数数据
    
    返回：
    - 带所有技术指标的DataFrame
    """
    # 转换为英文列名便于计算
    df = convert_to_english_for_calculation(df, is_index=is_index)
    
    # 确保数据按日期排序
    df = df.sort_values('date').reset_index(drop=True)
    
    # 计算各类指标
    df = calculate_ma(df)              # 包含新增的ma50, ma150, ma200
    df = calculate_rsi(df)
    df = calculate_macd(df)
    df = calculate_kdj(df)
    df = calculate_price_changes(df)
    df = calculate_volume_indicators(df)
    df = calculate_price_range(df)
    df = calculate_rs_raw(df)          # v4.0 新增：RS原始值
    
    # 控制小数位数
    df = round_indicators(df)
    
    # 转换回中文列名
    if is_index:
        rename_back = {
            'symbol': '指数代码',
            'name': '指数名称',
            'date': '日期',
            'open': '开盘',
            'high': '最高',
            'low': '最低',
            'close': '收盘',
            'volume': '成交量',
            'amount': '成交额',
            'turnover': '换手率',
            'pct_change': '涨跌幅',
        }
    else:
        rename_back = {
            'symbol': '股票代码',
            'name': '股票名称',
            'date': '日期',
            'open': '开盘',
            'high': '最高',
            'low': '最低',
            'close': '收盘',
            'volume': '成交量',
            'amount': '成交额',
            'turnover': '换手率',
            'pct_change': '涨跌幅',
        }
    
    df = df.rename(columns=rename_back)
    
    return df

# ============================================================
# 单个文件处理
# ============================================================

def process_single_file(file_path, is_index=False, output_dir=None):
    """
    处理单个文件（个股或指数）
    
    参数：
    - file_path: 输入文件路径
    - is_index: 是否为指数数据
    - output_dir: 输出目录
    """
    try:
        # 读取数据
        df = pd.read_parquet(file_path)
        
        # 标准化列名
        df = normalize_columns(df, is_index=is_index)
        
        # 计算所有指标
        df = calculate_all_indicators(df, is_index=is_index)
        
        # 保存结果
        output_file = output_dir / file_path.name
        df.to_parquet(output_file, index=False)
        
        return {
            'file': file_path.name,
            'rows': len(df),
            'success': True
        }
    
    except Exception as e:
        logger.error(f"{file_path.name}: 处理失败 - {e}")
        return {
            'file': file_path.name,
            'success': False,
            'error': str(e)
        }

# ============================================================
# 批量处理
# ============================================================

def process_batch(files, is_index=False, output_dir=None, desc="处理进度"):
    """批量处理文件"""
    stats = {
        'total': len(files),
        'success': 0,
        'failed': 0
    }
    
    if USE_MULTIPROCESSING and NUM_PROCESSES > 1 and not is_index:
        # 多进程处理（仅用于个股，指数文件少不需要）
        process_func = partial(process_single_file, is_index=is_index, output_dir=output_dir)
        
        with mp.Pool(processes=NUM_PROCESSES) as pool:
            results = list(tqdm(
                pool.imap(process_func, files),
                total=len(files),
                desc=desc
            ))
    else:
        # 单进程处理
        results = []
        for file_path in tqdm(files, desc=desc):
            result = process_single_file(file_path, is_index=is_index, output_dir=output_dir)
            results.append(result)
    
    # 统计结果
    stats['success'] = sum(1 for r in results if r and r.get('success', False))
    stats['failed'] = stats['total'] - stats['success']
    
    return stats, results

# ============================================================
# RS Rating 排名计算（v4.0 新增）
# ============================================================

def calculate_rs_rating_for_stocks():
    """
    计算个股的 RS Rating 排名（百分位）
    
    原理：
    1. 读取所有个股的 rs_raw 值
    2. 按每个交易日进行横向排名
    3. 转换为百分位（0-100分）
    4. 更新回各股票文件
    
    返回：
        dict: 统计信息
    """
    print("\n" + "=" * 80)
    print("第3部分：计算个股 RS Rating 排名")
    print("=" * 80)
    
    # 检查输出目录
    if not OUTPUT_STOCK_DIR.exists():
        print(f"❌ 个股指标目录不存在: {OUTPUT_STOCK_DIR}")
        return {'success': 0, 'failed': 0, 'total': 0}
    
    # 获取所有个股指标文件
    indicator_files = list(OUTPUT_STOCK_DIR.glob("*.parquet"))
    
    if not indicator_files:
        print(f"❌ 未找到个股指标文件: {OUTPUT_STOCK_DIR}")
        return {'success': 0, 'failed': 0, 'total': 0}
    
    print(f"找到 {len(indicator_files)} 个个股指标文件")
    
    # 1. 读取所有文件的 rs_raw 数据
    print(f"\n📖 读取数据...")
    
    all_data = []
    failed_reads = 0
    
    for file in tqdm(indicator_files, desc="读取进度"):
        try:
            # 只读取必要的列
            df = pd.read_parquet(file, columns=['股票代码', '日期', 'rs_raw'])
            all_data.append(df)
        except Exception as e:
            logger.error(f"{file.name}: 读取失败 - {e}")
            failed_reads += 1
    
    if not all_data:
        print("❌ 没有成功读取任何数据")
        return {'success': 0, 'failed': failed_reads, 'total': len(indicator_files)}
    
    print(f"✅ 成功读取 {len(all_data)} 个文件")
    if failed_reads > 0:
        print(f"⚠️  读取失败 {failed_reads} 个文件")
    
    # 2. 合并所有股票数据
    print(f"\n🔗 合并数据...")
    combined = pd.concat(all_data, ignore_index=True)
    
    print(f"  总记录数: {len(combined):,}")
    print(f"  股票数量: {combined['股票代码'].nunique():,}")
    print(f"  日期范围: {combined['日期'].min()} ~ {combined['日期'].max()}")
    
    # 3. 清理数据
    original_count = len(combined)
    combined = combined.dropna(subset=['rs_raw'])
    dropped_count = original_count - len(combined)
    
    if dropped_count > 0:
        print(f"\n  清理空值: 删除 {dropped_count:,} 条记录")
    
    if len(combined) == 0:
        print("❌ 没有有效的 rs_raw 数据")
        return {'success': 0, 'failed': failed_reads, 'total': len(indicator_files)}
    
    # 4. 按日期分组计算百分位排名
    print(f"\n📊 计算横向排名...")
    
    # 使用 rank(pct=True) 计算百分位，乘以100得到0-100分
    # method='min'：相同值取最小排名，更保守
    combined['rs_rating'] = combined.groupby('日期')['rs_raw'].rank(pct=True, method='min') * 100
    
    # 控制小数位数（1位小数）
    combined['rs_rating'] = combined['rs_rating'].round(DECIMAL_CONFIG['rs_rating'])
    
    unique_dates = combined['日期'].nunique()
    print(f"✅ 完成 {unique_dates:,} 个交易日的排名计算")
    
    # 显示排名分布
    print(f"\n  RS Rating 分布统计:")
    print(f"    最小值: {combined['rs_rating'].min():.1f}")
    print(f"    25分位: {combined['rs_rating'].quantile(0.25):.1f}")
    print(f"    中位数: {combined['rs_rating'].quantile(0.50):.1f}")
    print(f"    75分位: {combined['rs_rating'].quantile(0.75):.1f}")
    print(f"    最大值: {combined['rs_rating'].max():.1f}")
    
    # 5. 更新回各股票文件
    print(f"\n💾 更新文件...")
    
    grouped = combined.groupby('股票代码')
    
    success = 0
    failed = 0
    
    for symbol, group in tqdm(grouped, desc="更新进度"):
        try:
            file_path = OUTPUT_STOCK_DIR / f"{symbol}.parquet"
            
            if not file_path.exists():
                logger.warning(f"{symbol}: 文件不存在，跳过")
                failed += 1
                continue
            
            # 读取原文件
            df_original = pd.read_parquet(file_path)
            
            # 删除旧的 rs_rating（如果有）
            if 'rs_rating' in df_original.columns:
                df_original = df_original.drop(columns=['rs_rating'])
            
            # 合并新的 rs_rating
            df_updated = df_original.merge(
                group[['日期', 'rs_rating']], 
                on='日期', 
                how='left'
            )
            
            # 确保列顺序（rs_rating放在最后）
            cols = [col for col in df_updated.columns if col != 'rs_rating'] + ['rs_rating']
            df_updated = df_updated[cols]
            
            # 保存
            df_updated.to_parquet(file_path, index=False)
            success += 1
            
        except Exception as e:
            logger.error(f"{symbol}: 更新失败 - {e}")
            failed += 1
    
    # 6. 统计结果
    stats = {
        'total': len(indicator_files),
        'success': success,
        'failed': failed
    }
    
    print(f"\n✅ 个股 RS Rating 更新完成")
    print(f"  更新成功: {stats['success']}/{stats['total']}")
    if stats['failed'] > 0:
        print(f"  更新失败: {stats['failed']}")
    
    return stats

def calculate_rs_rating_for_indexes():
    """
    计算大盘指数的 RS Rating 排名（百分位）
    
    注：大盘指数之间也可以进行相对强度比较
    """
    print("\n" + "=" * 80)
    print("第4部分：计算大盘 RS Rating 排名")
    print("=" * 80)
    
    # 检查输出目录
    if not OUTPUT_INDEX_DIR.exists():
        print(f"❌ 大盘指标目录不存在: {OUTPUT_INDEX_DIR}")
        return {'success': 0, 'failed': 0, 'total': 0}
    
    # 获取所有指数指标文件
    indicator_files = list(OUTPUT_INDEX_DIR.glob("*.parquet"))
    
    if not indicator_files:
        print(f"⚠️  未找到大盘指标文件: {OUTPUT_INDEX_DIR}")
        return {'success': 0, 'failed': 0, 'total': 0}
    
    print(f"找到 {len(indicator_files)} 个大盘指标文件")
    
    # 1. 读取所有文件的 rs_raw 数据
    print(f"\n📖 读取数据...")
    
    all_data = []
    failed_reads = 0
    
    for file in tqdm(indicator_files, desc="读取进度"):
        try:
            # 只读取必要的列
            df = pd.read_parquet(file, columns=['指数代码', '日期', 'rs_raw'])
            # 临时重命名为统一格式
            df = df.rename(columns={'指数代码': '代码'})
            all_data.append(df)
        except Exception as e:
            logger.error(f"{file.name}: 读取失败 - {e}")
            failed_reads += 1
    
    if not all_data:
        print("❌ 没有成功读取任何数据")
        return {'success': 0, 'failed': failed_reads, 'total': len(indicator_files)}
    
    print(f"✅ 成功读取 {len(all_data)} 个文件")
    if failed_reads > 0:
        print(f"⚠️  读取失败 {failed_reads} 个文件")
    
    # 2. 合并所有指数数据
    print(f"\n🔗 合并数据...")
    combined = pd.concat(all_data, ignore_index=True)
    
    print(f"  总记录数: {len(combined):,}")
    print(f"  指数数量: {combined['代码'].nunique():,}")
    print(f"  日期范围: {combined['日期'].min()} ~ {combined['日期'].max()}")
    
    # 3. 清理数据
    original_count = len(combined)
    combined = combined.dropna(subset=['rs_raw'])
    dropped_count = original_count - len(combined)
    
    if dropped_count > 0:
        print(f"\n  清理空值: 删除 {dropped_count:,} 条记录")
    
    if len(combined) == 0:
        print("❌ 没有有效的 rs_raw 数据")
        return {'success': 0, 'failed': failed_reads, 'total': len(indicator_files)}
    
    # 4. 按日期分组计算百分位排名
    print(f"\n📊 计算横向排名...")
    
    combined['rs_rating'] = combined.groupby('日期')['rs_raw'].rank(pct=True, method='min') * 100
    combined['rs_rating'] = combined['rs_rating'].round(DECIMAL_CONFIG['rs_rating'])
    
    unique_dates = combined['日期'].nunique()
    print(f"✅ 完成 {unique_dates:,} 个交易日的排名计算")
    
    # 显示排名分布
    print(f"\n  RS Rating 分布统计:")
    print(f"    最小值: {combined['rs_rating'].min():.1f}")
    print(f"    25分位: {combined['rs_rating'].quantile(0.25):.1f}")
    print(f"    中位数: {combined['rs_rating'].quantile(0.50):.1f}")
    print(f"    75分位: {combined['rs_rating'].quantile(0.75):.1f}")
    print(f"    最大值: {combined['rs_rating'].max():.1f}")
    
    # 5. 更新回各指数文件
    print(f"\n💾 更新文件...")
    
    grouped = combined.groupby('代码')
    
    success = 0
    failed = 0
    
    for symbol, group in tqdm(grouped, desc="更新进度"):
        try:
            file_path = OUTPUT_INDEX_DIR / f"{symbol}.parquet"
            
            if not file_path.exists():
                logger.warning(f"{symbol}: 文件不存在，跳过")
                failed += 1
                continue
            
            # 读取原文件
            df_original = pd.read_parquet(file_path)
            
            # 删除旧的 rs_rating（如果有）
            if 'rs_rating' in df_original.columns:
                df_original = df_original.drop(columns=['rs_rating'])
            
            # 合并新的 rs_rating
            df_updated = df_original.merge(
                group[['日期', 'rs_rating']], 
                on='日期', 
                how='left'
            )
            
            # 确保列顺序（rs_rating放在最后）
            cols = [col for col in df_updated.columns if col != 'rs_rating'] + ['rs_rating']
            df_updated = df_updated[cols]
            
            # 保存
            df_updated.to_parquet(file_path, index=False)
            success += 1
            
        except Exception as e:
            logger.error(f"{symbol}: 更新失败 - {e}")
            failed += 1
    
    # 6. 统计结果
    stats = {
        'total': len(indicator_files),
        'success': success,
        'failed': failed
    }
    
    print(f"\n✅ 大盘 RS Rating 更新完成")
    print(f"  更新成功: {stats['success']}/{stats['total']}")
    if stats['failed'] > 0:
        print(f"  更新失败: {stats['failed']}")
    
    return stats

# ============================================================
# 主程序
# ============================================================

def main():
    """主函数"""
    print("=" * 80)
    print("  完整技术指标计算工具 v4.0 增强版")
    print("  新增：RS Raw + RS Rating + MA50/150/200")
    print("  保留：大盘指数处理 + 所有原有功能")
    print("=" * 80)
    
    # ========================================
    # 第1部分：处理个股数据
    # ========================================
    print("\n" + "=" * 80)
    print("第1部分：处理个股数据")
    print("=" * 80)
    
    if not INPUT_STOCK_DIR.exists():
        print(f"❌ 个股数据目录不存在: {INPUT_STOCK_DIR}")
        stock_stats = {'total': 0, 'success': 0, 'failed': 0}
    else:
        stock_files = list(INPUT_STOCK_DIR.glob("*.parquet"))
        print(f"找到 {len(stock_files)} 个个股文件")
        
        if stock_files:
            stock_stats, _ = process_batch(
                stock_files, 
                is_index=False, 
                output_dir=OUTPUT_STOCK_DIR,
                desc="个股处理"
            )
            
            print(f"\n✅ 个股处理完成")
            print(f"  成功: {stock_stats['success']}/{stock_stats['total']}")
            if stock_stats['failed'] > 0:
                print(f"  失败: {stock_stats['failed']}")
        else:
            print(f"❌ 未找到个股数据文件")
            stock_stats = {'total': 0, 'success': 0, 'failed': 0}
    
    # ========================================
    # 第2部分：处理大盘指数数据
    # ========================================
    print("\n" + "=" * 80)
    print("第2部分：处理大盘指数数据")
    print("=" * 80)
    
    if not INPUT_INDEX_DIR.exists():
        print(f"❌ 大盘数据目录不存在: {INPUT_INDEX_DIR}")
        index_stats = {'total': 0, 'success': 0, 'failed': 0}
    else:
        index_files = list(INPUT_INDEX_DIR.glob("*.parquet"))
        print(f"找到 {len(index_files)} 个指数文件")
        
        if index_files:
            index_stats, _ = process_batch(
                index_files, 
                is_index=True, 
                output_dir=OUTPUT_INDEX_DIR,
                desc="指数处理"
            )
            
            print(f"\n✅ 指数处理完成")
            print(f"  成功: {index_stats['success']}/{index_stats['total']}")
            if index_stats['failed'] > 0:
                print(f"  失败: {index_stats['failed']}")
        else:
            print(f"⚠️  未找到指数数据文件")
            index_stats = {'total': 0, 'success': 0, 'failed': 0}
    
    # ========================================
    # 第3部分：计算个股 RS Rating 排名
    # ========================================
    stock_rs_stats = {'success': 0, 'failed': 0, 'total': 0}
    if stock_stats['success'] > 0:
        stock_rs_stats = calculate_rs_rating_for_stocks()
    else:
        print("\n⚠️  跳过个股 RS Rating 计算（无成功处理的个股数据）")
    
    # ========================================
    # 第4部分：计算大盘 RS Rating 排名
    # ========================================
    index_rs_stats = {'success': 0, 'failed': 0, 'total': 0}
    if index_stats['success'] > 0:
        index_rs_stats = calculate_rs_rating_for_indexes()
    else:
        print("\n⚠️  跳过大盘 RS Rating 计算（无成功处理的大盘数据）")
    
    # ========================================
    # 完成统计
    # ========================================
    print("\n" + "=" * 80)
    print("计算完成统计")
    print("=" * 80)
    print(f"个股:")
    print(f"  总数: {stock_stats['total']}")
    print(f"  ✅ 指标计算成功: {stock_stats['success']}")
    print(f"  ✅ RS Rating成功: {stock_rs_stats['success']}")
    if stock_stats['failed'] > 0 or stock_rs_stats['failed'] > 0:
        print(f"  ❌ 失败: {stock_stats['failed'] + stock_rs_stats['failed']}")
    
    print(f"\n指数:")
    print(f"  总数: {index_stats['total']}")
    print(f"  ✅ 指标计算成功: {index_stats['success']}")
    print(f"  ✅ RS Rating成功: {index_rs_stats['success']}")
    if index_stats['failed'] > 0 or index_rs_stats['failed'] > 0:
        print(f"  ❌ 失败: {index_stats['failed'] + index_rs_stats['failed']}")
    
    print(f"\n总计:")
    total_success = stock_stats['success'] + index_stats['success'] + stock_rs_stats['success'] + index_rs_stats['success']
    total_failed = stock_stats['failed'] + index_stats['failed'] + stock_rs_stats['failed'] + index_rs_stats['failed']
    print(f"  ✅ 成功: {total_success}")
    if total_failed > 0:
        print(f"  ❌ 失败: {total_failed}")
    print("=" * 80)
    
    # ========================================
    # 数据示例展示
    # ========================================
    if stock_stats['success'] > 0:
        print("\n" + "=" * 80)
        print("📊 个股数据示例")
        print("=" * 80)
        
        sample_file = list(OUTPUT_STOCK_DIR.glob("*.parquet"))[0]
        sample_df = pd.read_parquet(sample_file)
        
        print(f"\n文件: {sample_file.name}")
        print(f"  数据行数: {len(sample_df):,}")
        print(f"  指标数量: {len(sample_df.columns)}")
        
        print(f"\n  最新5条记录:")
        display_cols = ['日期', '收盘', 'ma5', 'ma20', 'ma60', 'ma150', 'rsi14', 'rs_raw', 'rs_rating', 'change_60d']
        available_cols = [col for col in display_cols if col in sample_df.columns]
        print(sample_df[available_cols].tail(5).to_string(index=False))
    
    if index_stats['success'] > 0:
        print("\n" + "=" * 80)
        print("📊 指数数据示例")
        print("=" * 80)
        
        sample_file = list(OUTPUT_INDEX_DIR.glob("*.parquet"))[0]
        sample_df = pd.read_parquet(sample_file)
        
        print(f"\n文件: {sample_file.name}")
        print(f"  数据行数: {len(sample_df):,}")
        print(f"  指标数量: {len(sample_df.columns)}")
        
        print(f"\n  最新5条记录:")
        display_cols = ['日期', '收盘', 'ma5', 'ma20', 'ma60', 'ma150', 'rsi14', 'rs_raw', 'rs_rating', 'change_60d']
        available_cols = [col for col in display_cols if col in sample_df.columns]
        print(sample_df[available_cols].tail(5).to_string(index=False))
    
    # ========================================
    # 使用说明
    # ========================================
    print("\n" + "=" * 80)
    print("✅ 所有计算完成！")
    print("=" * 80)
    print(f"💾 个股指标: {OUTPUT_STOCK_DIR}")
    print(f"💾 大盘指标: {OUTPUT_INDEX_DIR}")
    print(f"📝 日志文件: {LOG_DIR}/calculate_indicators_*.log")
    
    print("\n" + "=" * 80)
    print("v4.0 新增功能")
    print("=" * 80)
    print("""
✅ 新增指标（个股和大盘都有）：
1. RS Raw（rs_raw）：相对强度原始值
   - 计算方式：加权平均多周期涨幅
   - 公式：0.4×change_20d + 0.3×change_60d + 0.2×change_120d + 0.1×change_250d
   - 用途：衡量个股/指数的综合强度

2. RS Rating（rs_rating）：相对强度百分位排名
   - 范围：0.0 - 100.0
   - 含义：该股票/指数强于多少百分比的同类标的
   - 计算：每个交易日对所有标的的rs_raw进行横向排名
   - 评级参考：
     * 80+ 分：强势股/强势指数
     * 60-80分：中等偏强
     * 40-60分：中性
     * 20-40分：中等偏弱
     * 0-20分：弱势股/弱势指数

3. 新增均线：
   - ma50：50日均线
   - ma150：150日均线
   - ma200：200日均线

✅ 保留的所有原有功能：
1. 移动平均线: ma5, ma10, ma20, ma30, ma60, ma120, ma250
2. RSI指标: rsi6, rsi12, rsi14, rsi24
3. MACD: macd_dif, macd_dea, macd, macd_bar
4. KDJ: kdj_k, kdj_d, kdj_j
5. 涨跌幅: change_1d, change_5d, change_10d, change_20d, change_25d, 
          change_30d, change_60d, change_120d, change_180d, change_250d
6. 成交量: volume_ma5, volume_ma10, volume_ma20, volume_ma30, volume_ratio_5d
7. 价格区间: high_20d, high_60d, high_120d, high_250d, high_52w,
            low_20d, low_60d, low_120d, low_250d, low_52w

使用示例：
```python
import pandas as pd

# 读取个股指标（含RS Rating）
stock_df = pd.read_parquet('data/technical_indicators/000001.parquet')
print(f"RS Rating: {stock_df['rs_rating'].iloc[-1]}")  # 最新RS评分
print(f"RS Raw: {stock_df['rs_raw'].iloc[-1]}")        # RS原始值
print(f"MA150: {stock_df['ma150'].iloc[-1]}")          # 150日均线

# 读取大盘指标（含RS Rating）
index_df = pd.read_parquet('data/index_indicators/000001.SH.parquet')
print(f"大盘RS Rating: {index_df['rs_rating'].iloc[-1]}")

# 筛选强势股（RS Rating > 80）
strong_stocks = stock_df[stock_df['rs_rating'] > 80]

# 判断股票是否跑赢大盘
if stock_df['change_60d'].iloc[-1] > index_df['change_60d'].iloc[-1]:
    print("该股票跑赢大盘！")
```

注意事项：
1. RS Rating是横向排名，每个交易日重新计算
2. 个股之间比较用个股的RS Rating
3. 指数之间比较用指数的RS Rating
4. RS Raw和RS Rating都需要至少250天历史数据
5. 所有原有功能和输出目录保持不变
    """)
    
    return 0 if total_success > 0 else 1

if __name__ == "__main__":
    exit(main())
