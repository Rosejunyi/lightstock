#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
直接下载前复权和后复权数据 v5.1 - 增量下载版（修复版）

修复内容：
1. ✅ 添加完整字段：涨跌额、振幅、昨收、涨跌幅异常、停牌
2. ✅ 自动计算派生字段
3. ✅ 兼容历史数据

作者：Claude
版本：v5.1
日期：2025-11-03
"""

import baostock as bs
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from tqdm import tqdm
import logging
import time
import shutil

# ============================================================
# 配置
# ============================================================

# 目录配置
QFQ_DATA_DIR = Path("data/daily_parquet_qfq")  # 前复权数据
HFQ_DATA_DIR = Path("data/daily_parquet_hfq")  # 后复权数据
STOCK_INFO_FILE = Path("data/stock_basic_info.parquet")  # 股票信息
BACKUP_DIR = Path("data/backups/adjusted_data")
LOG_DIR = Path("logs")

# 创建目录
QFQ_DATA_DIR.mkdir(parents=True, exist_ok=True)
HFQ_DATA_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 下载配置
DEFAULT_START_DATE = "1990-01-01"  # 首次下载的开始日期
END_DATE = datetime.now().strftime('%Y-%m-%d')  # 结束日期（今天）
MAX_RETRIES = 3
RETRY_DELAY = 2
BATCH_SIZE = 50

# 增量更新配置
INCREMENTAL_CONFIG = {
    'force_full_download': False,     # 是否强制全量下载
    'lookback_days': 10,              # 回溯天数：防止数据遗漏
    'min_gap_days': 1,                # 最小更新间隔：距离最新数据<N天不更新
    'backup_before_update': True,     # 更新前是否备份
}

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(
            LOG_DIR / f'adjusted_incremental_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
            encoding='utf-8'
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# 工具函数
# ============================================================

def add_market_prefix(pure_code):
    """添加市场前缀"""
    pure_code = str(pure_code).zfill(6)
    if pure_code.startswith('6') or pure_code.startswith('5'):
        return f'sh.{pure_code}'
    else:
        return f'sz.{pure_code}'

def format_date_string(date_value):
    """格式化日期为 YYYY-MM-DD 字符串"""
    if pd.isna(date_value):
        return None
    try:
        if isinstance(date_value, str):
            if len(date_value) == 10 and date_value[4] == '-' and date_value[7] == '-':
                return date_value
        dt = pd.to_datetime(date_value)
        return dt.strftime('%Y-%m-%d')
    except:
        return None

def calculate_derived_fields(df):
    """
    计算派生字段
    
    新增字段：
    - 涨跌额：收盘 - 昨收
    - 振幅：(最高 - 最低) / 昨收 * 100
    - 昨收：前一天的收盘价
    """
    if df.empty:
        return df
    
    # 确保数据按日期排序
    df = df.sort_values('日期').reset_index(drop=True)
    
    # 计算昨收（前一天的收盘价）
    df['昨收'] = df['收盘'].shift(1)
    
    # 计算涨跌额
    df['涨跌额'] = df['收盘'] - df['昨收']
    
    # 计算振幅
    df['振幅'] = ((df['最高'] - df['最低']) / df['昨收'] * 100).round(4)
    
    # 判断涨跌幅异常（涨跌幅超过±10%，或ST股超过±5%）
    # 简化版本：涨跌幅 > 10% 或 < -10% 标记为异常
    df['涨跌幅异常'] = df['涨跌幅'].apply(
        lambda x: 'X' if (pd.notna(x) and (abs(x) > 10)) else None
    )
    
    # 停牌判断（成交量为0视为停牌）
    df['停牌'] = df['成交量'].apply(lambda x: 'X' if (pd.notna(x) and x == 0) else None)
    
    return df

def get_existing_data(stock_code, data_dir):
    """
    读取现有数据
    
    返回: (DataFrame, 最新日期)
    """
    file_path = data_dir / f"{stock_code}.parquet"
    
    if not file_path.exists():
        return None, None
    
    try:
        df = pd.read_parquet(file_path)
        
        if df.empty:
            return None, None
        
        # 获取最新日期
        df['日期'] = pd.to_datetime(df['日期'])
        latest_date = df['日期'].max().strftime('%Y-%m-%d')
        
        logger.debug(f"{stock_code}: 现有数据 {len(df)} 条，最新日期 {latest_date}")
        
        return df, latest_date
    
    except Exception as e:
        logger.error(f"{stock_code}: 读取现有数据失败 - {e}")
        return None, None

def calculate_download_range(latest_date, stock_code):
    """
    计算需要下载的日期范围
    
    返回: (开始日期, 结束日期, 是否需要下载)
    """
    if INCREMENTAL_CONFIG['force_full_download']:
        return DEFAULT_START_DATE, END_DATE, True
    
    if latest_date is None:
        # 首次下载
        return DEFAULT_START_DATE, END_DATE, True
    
    # 解析最新日期
    latest_dt = datetime.strptime(latest_date, '%Y-%m-%d')
    today = datetime.now()
    
    # 检查是否需要更新
    days_gap = (today - latest_dt).days
    
    if days_gap <= INCREMENTAL_CONFIG['min_gap_days']:
        logger.debug(f"{stock_code}: 数据已是最新（距今{days_gap}天），跳过下载")
        return None, None, False
    
    # 计算下载范围（回溯N天防止遗漏）
    start_dt = latest_dt - timedelta(days=INCREMENTAL_CONFIG['lookback_days'])
    start_date = start_dt.strftime('%Y-%m-%d')
    
    logger.debug(f"{stock_code}: 增量下载 {start_date} 至 {END_DATE}")
    
    return start_date, END_DATE, True

def download_adjusted_data(stock_code, adjust_type='qfq', start_date=None, end_date=None, retry_count=0):
    """
    下载单只股票的复权数据
    
    参数:
        stock_code: 股票代码（6位数字）
        adjust_type: 'qfq' (前复权) 或 'hfq' (后复权)
        start_date: 开始日期
        end_date: 结束日期
        retry_count: 当前重试次数
    
    返回: 
        DataFrame 或 None
    """
    try:
        # 添加市场前缀
        bs_code = add_market_prefix(stock_code)
        
        # 使用传入的日期范围
        if start_date is None:
            start_date = DEFAULT_START_DATE
        if end_date is None:
            end_date = END_DATE
        
        # 设置复权参数
        adjustflag = '2' if adjust_type == 'qfq' else '1'  # 2=前复权, 1=后复权
        
        # 调用 Baostock API - 请求所有可用字段
        rs = bs.query_history_k_data_plus(
            code=bs_code,
            fields="date,open,high,low,close,preclose,volume,amount,turn,pctChg,isST",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag=adjustflag
        )
        
        # 检查返回状态
        if rs.error_code != '0':
            if retry_count < MAX_RETRIES:
                logger.debug(f"{stock_code}: 下载失败，重试 {retry_count + 1}/{MAX_RETRIES}")
                time.sleep(RETRY_DELAY)
                return download_adjusted_data(stock_code, adjust_type, start_date, end_date, retry_count + 1)
            else:
                logger.debug(f"{stock_code}: {rs.error_msg}")
                return None
        
        # 收集数据
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        # 检查是否有数据
        if not data_list:
            logger.debug(f"{stock_code}: 无复权数据")
            return pd.DataFrame()  # 返回空DataFrame
        
        # 转换为 DataFrame
        df = pd.DataFrame(data_list, columns=rs.fields)
        
        # 重命名列
        column_mapping = {
            'date': '日期',
            'open': '开盘',
            'high': '最高',
            'low': '最低',
            'close': '收盘',
            'preclose': '昨收',
            'volume': '成交量',
            'amount': '成交额',
            'turn': '换手率',
            'pctChg': '涨跌幅',
            'isST': 'ST标记'
        }
        
        df = df.rename(columns=column_mapping)
        
        # 转换数据类型
        numeric_columns = ['开盘', '最高', '最低', '收盘', '昨收', '成交额', '换手率', '涨跌幅']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if '成交量' in df.columns:
            df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce').astype('Int64')
        
        # 格式化日期
        df['日期'] = df['日期'].apply(format_date_string)
        
        # 删除无效行
        df = df.dropna(subset=['日期'])
        
        if df.empty:
            logger.debug(f"{stock_code}: 转换后无有效数据")
            return pd.DataFrame()
        
        # 计算派生字段
        df = calculate_derived_fields(df)
        
        return df
    
    except Exception as e:
        if retry_count < MAX_RETRIES:
            logger.debug(f"{stock_code}: 异常，重试 {retry_count + 1}/{MAX_RETRIES} - {e}")
            time.sleep(RETRY_DELAY)
            return download_adjusted_data(stock_code, adjust_type, start_date, end_date, retry_count + 1)
        else:
            logger.error(f"{stock_code}: 下载异常 - {e}")
            return None

def merge_data(df_existing, df_new, stock_code, stock_info):
    """
    合并现有数据和新下载的数据
    
    策略：
    1. 合并两个DataFrame
    2. 按日期去重（保留新数据）
    3. 重新计算所有派生字段（确保数据一致性）
    4. 排序
    5. 添加股票信息
    """
    if df_existing is None or df_existing.empty:
        df_result = df_new
    elif df_new is None or df_new.empty:
        return df_existing
    else:
        # 确保日期格式一致
        df_existing['日期'] = pd.to_datetime(df_existing['日期'])
        df_new['日期'] = pd.to_datetime(df_new['日期'])
        
        # 合并
        df_result = pd.concat([df_existing, df_new], ignore_index=True)
        
        # 去重（保留最新的）
        df_result = df_result.drop_duplicates(subset=['日期'], keep='last')
        
        # 排序
        df_result = df_result.sort_values('日期').reset_index(drop=True)
    
    # 转换日期回字符串
    df_result['日期'] = df_result['日期'].dt.strftime('%Y-%m-%d')
    
    # 重新计算派生字段（确保完整性）
    df_result = calculate_derived_fields(df_result)
    
    # 添加股票代码
    df_result['股票代码'] = stock_code
    
    # 添加股票名称
    if stock_info is not None:
        stock_row = stock_info[stock_info['股票代码'] == stock_code]
        if not stock_row.empty:
            df_result['股票名称'] = stock_row['股票名称'].iloc[0]
    
    return df_result

def get_stock_list():
    """获取需要处理的股票列表"""
    # 优先从已有数据中获取
    stock_codes = set()
    
    # 从前复权目录获取
    if QFQ_DATA_DIR.exists():
        qfq_files = list(QFQ_DATA_DIR.glob("*.parquet"))
        stock_codes.update([f.stem for f in qfq_files])
    
    # 从后复权目录获取
    if HFQ_DATA_DIR.exists():
        hfq_files = list(HFQ_DATA_DIR.glob("*.parquet"))
        stock_codes.update([f.stem for f in hfq_files])
    
    # 如果没有已有数据，从股票信息文件获取
    if not stock_codes and STOCK_INFO_FILE.exists():
        try:
            stock_info = pd.read_parquet(STOCK_INFO_FILE)
            stock_codes = set(stock_info['股票代码'].astype(str).tolist())
        except Exception as e:
            logger.error(f"读取股票信息失败: {e}")
    
    stock_codes = sorted(list(stock_codes))
    logger.info(f"✅ 获取到 {len(stock_codes)} 只股票")
    
    return stock_codes

def load_stock_info():
    """加载股票信息"""
    try:
        if not STOCK_INFO_FILE.exists():
            logger.warning(f"⚠️  股票信息文件不存在: {STOCK_INFO_FILE}")
            return None
        
        logger.info(f"加载股票信息: {STOCK_INFO_FILE}")
        stock_info = pd.read_parquet(STOCK_INFO_FILE)
        
        # 确保股票代码是字符串格式
        if '股票代码' in stock_info.columns:
            stock_info['股票代码'] = stock_info['股票代码'].astype(str).str.zfill(6)
        
        logger.info(f"✅ 股票信息数据: {len(stock_info)} 只股票")
        
        return stock_info
    except Exception as e:
        logger.error(f"❌ 加载股票信息失败: {e}")
        return None

def backup_file(file_path):
    """备份单个文件"""
    if not file_path.exists():
        return
    
    try:
        # 创建股票代码对应的备份目录
        stock_code = file_path.stem
        backup_subdir = BACKUP_DIR / stock_code
        backup_subdir.mkdir(parents=True, exist_ok=True)
        
        # 生成备份文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = backup_subdir / f"{stock_code}_{timestamp}.parquet"
        
        # 复制文件
        shutil.copy2(file_path, backup_file)
        logger.debug(f"已备份: {backup_file}")
        
        # 清理旧备份（保留最近3个）
        backups = sorted(backup_subdir.glob(f"{stock_code}_*.parquet"), reverse=True)
        for old_backup in backups[3:]:
            old_backup.unlink()
            logger.debug(f"删除旧备份: {old_backup}")
    
    except Exception as e:
        logger.warning(f"备份失败: {e}")

# ============================================================
# 主程序
# ============================================================

def main():
    print("=" * 80)
    print("  下载前复权和后复权数据 v5.1 - 增量下载版（修复版）")
    print("  智能检测 + 增量更新 + 完整字段 + 数据验证")
    print("=" * 80)
    
    print(f"\n配置:")
    print(f"  强制全量下载: {'是' if INCREMENTAL_CONFIG['force_full_download'] else '否'}")
    print(f"  回溯天数: {INCREMENTAL_CONFIG['lookback_days']} 天")
    print(f"  最小更新间隔: {INCREMENTAL_CONFIG['min_gap_days']} 天")
    print(f"  结束日期: {END_DATE}")
    print(f"  更新前备份: {'是' if INCREMENTAL_CONFIG['backup_before_update'] else '否'}")
    
    print(f"\n✨ 新增功能:")
    print(f"  ✅ 自动计算：涨跌额、振幅、昨收")
    print(f"  ✅ 智能判断：涨跌幅异常、停牌状态")
    
    # 获取股票列表
    print("\n步骤 1/5: 获取股票列表...")
    stock_codes = get_stock_list()
    
    if not stock_codes:
        print("❌ 无法获取股票列表")
        print("提示: 请确保已有数据或股票信息文件存在")
        return
    
    print(f"✅ 获取到 {len(stock_codes)} 只股票")
    
    # 加载股票信息
    print("\n步骤 2/5: 加载股票信息...")
    stock_info = load_stock_info()
    if stock_info is not None:
        print(f"✅ 股票信息加载成功: {len(stock_info)} 只股票")
    else:
        print(f"⚠️  未加载股票信息，股票名称字段将为空")
    
    # 登录 Baostock
    print("\n步骤 3/5: 登录 Baostock...")
    lg = bs.login()
    if lg.error_code != '0':
        logger.error(f"❌ 登录失败: {lg.error_msg}")
        print(f"❌ 登录失败: {lg.error_msg}")
        return
    
    logger.info("✅ 登录成功")
    print("✅ 登录成功")
    
    # 统计信息
    stats = {
        'total': len(stock_codes),
        'qfq_updated': 0,
        'qfq_skipped': 0,
        'qfq_failed': 0,
        'hfq_updated': 0,
        'hfq_skipped': 0,
        'hfq_failed': 0,
        'new_records': 0
    }
    
    # 下载数据
    print(f"\n步骤 4/5: 增量更新复权数据...")
    print(f"提示：只下载缺失的交易日数据，并自动计算派生字段\n")
    
    with tqdm(total=len(stock_codes), desc="更新进度") as pbar:
        for i, stock_code in enumerate(stock_codes):
            # === 处理前复权数据 ===
            df_qfq_existing, latest_qfq_date = get_existing_data(stock_code, QFQ_DATA_DIR)
            start_date, end_date, need_download = calculate_download_range(latest_qfq_date, stock_code)
            
            if need_download:
                # 备份
                if INCREMENTAL_CONFIG['backup_before_update'] and df_qfq_existing is not None:
                    qfq_file = QFQ_DATA_DIR / f"{stock_code}.parquet"
                    backup_file(qfq_file)
                
                # 下载新数据
                df_qfq_new = download_adjusted_data(stock_code, 'qfq', start_date, end_date)
                
                if df_qfq_new is not None:
                    # 合并数据
                    df_qfq_final = merge_data(df_qfq_existing, df_qfq_new, stock_code, stock_info)
                    
                    # 保存
                    output_file = QFQ_DATA_DIR / f"{stock_code}.parquet"
                    df_qfq_final.to_parquet(output_file, index=False)
                    
                    new_records = len(df_qfq_new) if not df_qfq_new.empty else 0
                    stats['new_records'] += new_records
                    stats['qfq_updated'] += 1
                else:
                    stats['qfq_failed'] += 1
            else:
                stats['qfq_skipped'] += 1
            
            # === 处理后复权数据 ===
            df_hfq_existing, latest_hfq_date = get_existing_data(stock_code, HFQ_DATA_DIR)
            start_date, end_date, need_download = calculate_download_range(latest_hfq_date, stock_code)
            
            if need_download:
                # 备份
                if INCREMENTAL_CONFIG['backup_before_update'] and df_hfq_existing is not None:
                    hfq_file = HFQ_DATA_DIR / f"{stock_code}.parquet"
                    backup_file(hfq_file)
                
                # 下载新数据
                df_hfq_new = download_adjusted_data(stock_code, 'hfq', start_date, end_date)
                
                if df_hfq_new is not None:
                    # 合并数据
                    df_hfq_final = merge_data(df_hfq_existing, df_hfq_new, stock_code, stock_info)
                    
                    # 保存
                    output_file = HFQ_DATA_DIR / f"{stock_code}.parquet"
                    df_hfq_final.to_parquet(output_file, index=False)
                    
                    stats['hfq_updated'] += 1
                else:
                    stats['hfq_failed'] += 1
            else:
                stats['hfq_skipped'] += 1
            
            pbar.update(1)
            
            # 每处理一批显示统计
            if (i + 1) % BATCH_SIZE == 0:
                pbar.set_postfix({
                    'QFQ更新': stats['qfq_updated'],
                    'HFQ更新': stats['hfq_updated'],
                    '新增': stats['new_records']
                })
    
    # 退出登录
    bs.logout()
    logger.info("已退出 Baostock")
    
    # 验证结果
    print(f"\n步骤 5/5: 验证结果...")
    qfq_count = len(list(QFQ_DATA_DIR.glob("*.parquet")))
    hfq_count = len(list(HFQ_DATA_DIR.glob("*.parquet")))
    
    print(f"✅ 前复权文件数: {qfq_count}")
    print(f"✅ 后复权文件数: {hfq_count}")
    
    # 输出统计
    print("\n" + "=" * 80)
    print("更新完成统计")
    print("=" * 80)
    print(f"总股票数: {stats['total']}")
    print(f"\n前复权数据:")
    print(f"  ✅ 已更新: {stats['qfq_updated']}")
    print(f"  ⏭️  已跳过: {stats['qfq_skipped']} (数据已是最新)")
    print(f"  ❌ 更新失败: {stats['qfq_failed']}")
    print(f"\n后复权数据:")
    print(f"  ✅ 已更新: {stats['hfq_updated']}")
    print(f"  ⏭️  已跳过: {stats['hfq_skipped']} (数据已是最新)")
    print(f"  ❌ 更新失败: {stats['hfq_failed']}")
    print(f"\n📊 新增记录: {stats['new_records']} 条")
    print("=" * 80)
    
    # 显示数据示例
    if qfq_count > 0:
        print(f"\n📊 数据示例（前复权，最新5条）:")
        sample_file = list(QFQ_DATA_DIR.glob("*.parquet"))[0]
        sample_df = pd.read_parquet(sample_file)
        print(f"  股票: {sample_file.stem}")
        print(f"  数据行数: {len(sample_df):,}")
        print(f"  日期范围: {sample_df['日期'].min()} 至 {sample_df['日期'].max()}")
        
        display_cols = ['日期', '开盘', '收盘', '涨跌额', '涨跌幅', '振幅', '昨收']
        available_cols = [col for col in display_cols if col in sample_df.columns]
        print(sample_df[available_cols].tail(5).to_string(index=False))
    
    print("\n🎉 复权数据增量更新完成！")
    print(f"💡 数据目录: {QFQ_DATA_DIR} 和 {HFQ_DATA_DIR}")
    print(f"💡 备份目录: {BACKUP_DIR}")
    print(f"📝 日志文件: {LOG_DIR}/adjusted_incremental_*.log")
    
    # 使用建议
    print("\n" + "=" * 80)
    print("修复说明")
    print("=" * 80)
    print("""
✅ 已修复的字段：
   - 涨跌额：自动计算（收盘 - 昨收）
   - 振幅：自动计算（(最高 - 最低) / 昨收 × 100）
   - 昨收：从API获取 + 自动计算
   - 涨跌幅异常：智能判断（涨跌幅 > ±10%）
   - 停牌：智能判断（成交量 = 0）

💡 注意事项：
   1. 运行后会自动补全所有历史数据的派生字段
   2. 第一行数据的"昨收"、"涨跌额"、"振幅"会为空（正常）
   3. 10月17日之前的数据也会被重新计算，确保一致性
    """)

if __name__ == "__main__":
    main()
