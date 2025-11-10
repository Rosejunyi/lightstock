#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
下载大盘指数数据 v2.1 - 增量下载版（修复版）

修复内容：
1. ✅ 修复日期类型转换错误（第352行）
2. ✅ 增强智能跳过：考虑交易日而非自然日
3. ✅ 添加数据日期检查：避免重复下载

作者：Claude
版本：v2.1
日期：2025-11-10
"""

import baostock as bs
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from tqdm import tqdm
import logging
import shutil

# ============================================================
# 配置
# ============================================================

# 大盘指数配置
INDICES = {
    'sh.000001': {'name': '上证指数', 'code': '999999', 'download_code': 'sh.000001'},
    'sz.399001': {'name': '深证成指', 'code': '399001', 'download_code': 'sz.399001'},
    'sh.000300': {'name': '沪深300', 'code': '000300', 'download_code': 'sh.000300'},
    'sz.399006': {'name': '创业板指', 'code': '399006', 'download_code': 'sz.399006'},
}

# 目录配置
OUTPUT_DIR = Path("data/index_data")
BACKUP_DIR = Path("data/backups/index_data")
LOG_DIR = Path("logs")

# 下载配置
DEFAULT_START_DATE = "1990-01-01"
END_DATE = datetime.now().strftime('%Y-%m-%d')
MAX_RETRIES = 3
RETRY_DELAY = 2

# 增量更新配置 - 优化版
INCREMENTAL_CONFIG = {
    'force_full_download': False,
    'lookback_days': 10,
    'min_gap_days': 1,  # 距离最新数据<N天不更新
    'backup_before_update': True,
    'smart_trading_day_check': True,  # ✨ 新增：智能交易日检测
}

# 创建目录
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(
            LOG_DIR / f'index_incremental_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
            encoding='utf-8'
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# 工具函数
# ============================================================

def is_trading_day_today():
    """
    判断今天是否是交易日
    
    简单规则：
    - 周六日：非交易日
    - 工作日：可能是交易日（不考虑节假日）
    
    返回: (是否是交易日, 距离上个交易日的自然日天数)
    """
    today = datetime.now()
    weekday = today.weekday()  # 0=周一, 6=周日
    
    if weekday >= 5:  # 周六或周日
        # 计算距离上个周五的天数
        days_since_friday = weekday - 4
        return False, days_since_friday
    
    # 工作日，假设是交易日
    return True, 0

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
    - 波动异常：单日涨跌幅超过±5%
    """
    if df.empty:
        return df
    
    # 确保数据按日期排序
    df = df.sort_values('日期').reset_index(drop=True)
    
    # 如果API没有提供昨收，使用前一天的收盘价计算
    if '昨收' not in df.columns or df['昨收'].isna().all():
        df['昨收'] = df['收盘'].shift(1)
    
    # 计算涨跌额
    df['涨跌额'] = df['收盘'] - df['昨收']
    
    # 计算振幅
    df['振幅'] = ((df['最高'] - df['最低']) / df['昨收'] * 100).round(4)
    
    # 判断波动异常
    df['波动异常'] = df['涨跌幅'].apply(
        lambda x: 'X' if (pd.notna(x) and (abs(x) > 5)) else None
    )
    
    return df

def calculate_period_returns(df):
    """计算不同周期的收益率"""
    df = df.copy()
    df['日期'] = pd.to_datetime(df['日期'])
    df = df.sort_values('日期')
    
    df['涨跌幅_1月'] = df['收盘'].pct_change(22) * 100
    df['涨跌幅_3月'] = df['收盘'].pct_change(66) * 100
    df['涨跌幅_6月'] = df['收盘'].pct_change(132) * 100
    
    # 转换回字符串日期
    df['日期'] = df['日期'].dt.strftime('%Y-%m-%d')
    
    return df

def get_existing_data(index_code):
    """
    读取现有数据
    
    返回: (DataFrame, 最新日期)
    """
    file_path = OUTPUT_DIR / f"{index_code}.parquet"
    
    if not file_path.exists():
        return None, None
    
    try:
        df = pd.read_parquet(file_path)
        
        if df.empty:
            return None, None
        
        # 获取最新日期
        df['日期'] = pd.to_datetime(df['日期'])
        latest_date = df['日期'].max().strftime('%Y-%m-%d')
        
        logger.debug(f"{index_code}: 现有数据 {len(df)} 条，最新日期 {latest_date}")
        
        return df, latest_date
    
    except Exception as e:
        logger.error(f"{index_code}: 读取现有数据失败 - {e}")
        return None, None

def calculate_download_range(latest_date, index_name):
    """
    计算需要下载的日期范围（智能版）
    
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
    
    # 计算自然日间隔
    days_gap = (today - latest_dt).days
    
    # 智能交易日检测
    if INCREMENTAL_CONFIG['smart_trading_day_check']:
        is_trading, days_since_last_trading = is_trading_day_today()
        
        # 如果今天不是交易日，调整预期间隔
        if not is_trading:
            # 例如：周六运行，上个交易日是周五，数据应该是周五的
            # 那么实际间隔应该是1天（周五到周六）
            # 如果数据最新日期是周五，则不需要更新
            if days_gap <= (1 + days_since_last_trading):
                logger.info(f"⏭️  {index_name}: 今天非交易日，数据已是最新（最新：{latest_date}，距今{days_gap}天）")
                return None, None, False
        else:
            # 今天是交易日
            if days_gap <= INCREMENTAL_CONFIG['min_gap_days']:
                logger.info(f"⏭️  {index_name}: 数据已是最新（最新：{latest_date}，距今{days_gap}天）")
                return None, None, False
    else:
        # 简单检查
        if days_gap <= INCREMENTAL_CONFIG['min_gap_days']:
            logger.debug(f"{index_name}: 数据已是最新（距今{days_gap}天），跳过下载")
            return None, None, False
    
    # 计算下载范围（回溯N天防止遗漏）
    start_dt = latest_dt - timedelta(days=INCREMENTAL_CONFIG['lookback_days'])
    start_date = start_dt.strftime('%Y-%m-%d')
    
    logger.info(f"✅ {index_name}: 需要更新（最新：{latest_date} → 今天：{END_DATE}），增量下载 {start_date} 至 {END_DATE}")
    
    return start_date, END_DATE, True

def download_index_data(download_code, index_name, save_code, start_date, end_date, retry_count=0):
    """
    下载单个指数的历史数据
    """
    try:
        logger.info(f"下载 {index_name} ({start_date} 至 {end_date})")
        
        rs = bs.query_history_k_data_plus(
            code=download_code,
            fields="date,open,high,low,close,preclose,volume,amount,pctChg",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3"
        )
        
        if rs.error_code != '0':
            if retry_count < MAX_RETRIES:
                logger.warning(f"{index_name}: 下载失败，重试 {retry_count + 1}/{MAX_RETRIES}")
                import time
                time.sleep(RETRY_DELAY)
                return download_index_data(download_code, index_name, save_code, start_date, end_date, retry_count + 1)
            else:
                logger.error(f"{index_name}: {rs.error_msg}")
                return None
        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list:
            logger.warning(f"{index_name}: 无数据")
            return pd.DataFrame()
        
        df = pd.DataFrame(data_list, columns=rs.fields)
        
        column_mapping = {
            'date': '日期',
            'open': '开盘',
            'high': '最高',
            'low': '最低',
            'close': '收盘',
            'preclose': '昨收',
            'volume': '成交量',
            'amount': '成交额',
            'pctChg': '涨跌幅'
        }
        
        df = df.rename(columns=column_mapping)
        
        # 转换数值列
        numeric_cols = ['开盘', '最高', '最低', '收盘', '昨收', '成交量', '成交额', '涨跌幅']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 添加指数标识
        df['指数代码'] = save_code
        df['指数名称'] = index_name
        
        logger.info(f"{index_name}: 下载成功，共 {len(df)} 条记录")
        
        return df
    
    except Exception as e:
        logger.error(f"{index_name}: 下载异常 - {e}")
        return None

def merge_data(df_existing, df_new, index_name):
    """
    合并新旧数据（修复版）
    
    ✅ 修复：确保日期列在使用.dt之前是datetime类型
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
    
    # ✅ 修复：在使用.dt之前确保日期列是datetime类型
    if df_result['日期'].dtype != 'datetime64[ns]':
        df_result['日期'] = pd.to_datetime(df_result['日期'])
    
    # 转换日期回字符串
    df_result['日期'] = df_result['日期'].dt.strftime('%Y-%m-%d')
    
    # 重新计算派生字段
    df_result = calculate_derived_fields(df_result)
    
    # 重新计算周期收益率
    df_result = calculate_period_returns(df_result)
    
    logger.info(f"{index_name}: 合并后共 {len(df_result)} 条记录")
    
    return df_result

def backup_file(file_path, index_name):
    """备份单个文件"""
    if not file_path.exists():
        return
    
    try:
        index_code = file_path.stem
        backup_subdir = BACKUP_DIR / index_code
        backup_subdir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = backup_subdir / f"{index_code}_{timestamp}.parquet"
        
        shutil.copy2(file_path, backup_file)
        logger.debug(f"已备份: {backup_file}")
        
        # 清理旧备份（保留最近3个）
        backups = sorted(backup_subdir.glob(f"{index_code}_*.parquet"), reverse=True)
        for old_backup in backups[3:]:
            old_backup.unlink()
            logger.debug(f"删除旧备份: {old_backup}")
    
    except Exception as e:
        logger.warning(f"{index_name} 备份失败: {e}")

# ============================================================
# 主程序
# ============================================================

def main():
    print("=" * 80)
    print("  下载大盘指数数据 v2.1 - 增量下载版（修复版）")
    print("  ✅ 修复日期类型错误")
    print("  ✅ 智能交易日检测")
    print("  ✅ 避免重复下载")
    print("=" * 80)
    
    # 检查今天是否是交易日
    is_trading, days_since = is_trading_day_today()
    today_str = datetime.now().strftime('%Y-%m-%d (%A)')
    
    if is_trading:
        print(f"\n📅 今天: {today_str} - 交易日")
    else:
        print(f"\n📅 今天: {today_str} - 非交易日（距离上个交易日约{days_since}天）")
    
    print(f"\n配置:")
    print(f"  强制全量下载: {'是' if INCREMENTAL_CONFIG['force_full_download'] else '否'}")
    print(f"  回溯天数: {INCREMENTAL_CONFIG['lookback_days']} 天")
    print(f"  最小更新间隔: {INCREMENTAL_CONFIG['min_gap_days']} 天")
    print(f"  智能交易日检测: {'是' if INCREMENTAL_CONFIG['smart_trading_day_check'] else '否'}")
    print(f"  结束日期: {END_DATE}")
    
    print(f"\n指数列表:")
    for key, index_info in INDICES.items():
        print(f"  - {index_info['name']} (保存代码: {index_info['code']})")
    
    # 登录 Baostock
    print("\n登录 Baostock...")
    lg = bs.login()
    if lg.error_code != '0':
        logger.error(f"❌ 登录失败: {lg.error_msg}")
        print(f"❌ 登录失败: {lg.error_msg}")
        return
    
    logger.info("✅ 登录成功")
    print("✅ 登录成功")
    
    # 统计信息
    stats = {
        'total': len(INDICES),
        'updated': 0,
        'skipped': 0,
        'failed': 0,
        'new_records': 0
    }
    
    # 下载指数数据
    print(f"\n开始智能增量更新...\n")
    
    for key, index_info in tqdm(INDICES.items(), desc="更新进度"):
        index_name = index_info['name']
        save_code = index_info['code']
        download_code = index_info['download_code']
        
        # 读取现有数据
        df_existing, latest_date = get_existing_data(save_code)
        
        # 计算下载范围
        start_date, end_date, need_download = calculate_download_range(latest_date, index_name)
        
        if not need_download:
            stats['skipped'] += 1
            continue
        
        # 备份
        if INCREMENTAL_CONFIG['backup_before_update'] and df_existing is not None:
            file_path = OUTPUT_DIR / f"{save_code}.parquet"
            backup_file(file_path, index_name)
        
        # 下载新数据
        df_new = download_index_data(download_code, index_name, save_code, start_date, end_date)
        
        if df_new is None:
            stats['failed'] += 1
            continue
        
        # 合并数据
        df_final = merge_data(df_existing, df_new, index_name)
        
        if df_final is None or df_final.empty:
            stats['failed'] += 1
            continue
        
        # 保存Parquet格式
        output_file = OUTPUT_DIR / f"{save_code}.parquet"
        df_final.to_parquet(output_file, index=False)
        
        # 同时保存CSV版本
        csv_file = OUTPUT_DIR / f"{save_code}.csv"
        df_final.to_csv(csv_file, index=False, encoding='utf-8-sig')
        
        new_records = len(df_new) if not df_new.empty else 0
        stats['new_records'] += new_records
        stats['updated'] += 1
    
    # 退出登录
    bs.logout()
    logger.info("已退出 Baostock")
    
    # 输出统计
    print("\n" + "=" * 80)
    print("更新完成统计")
    print("=" * 80)
    print(f"总指数数: {stats['total']}")
    print(f"✅ 已更新: {stats['updated']}")
    print(f"⏭️  已跳过: {stats['skipped']} (数据已是最新)")
    print(f"❌ 更新失败: {stats['failed']}")
    print(f"📊 新增记录: {stats['new_records']} 条")
    print("=" * 80)
    
    if stats['updated'] > 0 or stats['skipped'] > 0:
        print(f"\n✅ 指数数据已是最新状态")
        print(f"💡 数据目录: {OUTPUT_DIR}")
        print(f"💡 备份目录: {BACKUP_DIR}")
    else:
        logger.error("❌ 没有成功更新任何指数数据")

if __name__ == "__main__":
    main()
