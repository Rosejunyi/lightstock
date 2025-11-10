#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
下载大盘指数数据 v2.0 - 增量下载版（含完整字段）

功能：
1. ✅ 下载上证指数、深证成指、沪深300、创业板指
2. ✅ 增量下载：只下载缺失的交易日数据
3. ✅ 完整字段：涨跌额、振幅、昨收等派生字段
4. ✅ 周期收益：1月、3月、6月涨跌幅
5. ✅ 自动备份：保留历史版本
6. ✅ 数据验证：检查数据完整性

用途：
- 计算相对强度(RS Rating)
- 相对大盘表现分析
- 市场环境判断

数据源：Baostock
作者：Claude
版本：v2.0（增量版）
日期：2025-11-03
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
DEFAULT_START_DATE = "1990-01-01"  # 首次下载的开始日期
END_DATE = datetime.now().strftime('%Y-%m-%d')  # 结束日期（今天）
MAX_RETRIES = 3
RETRY_DELAY = 2

# 增量更新配置
INCREMENTAL_CONFIG = {
    'force_full_download': False,     # 是否强制全量下载
    'lookback_days': 10,              # 回溯天数：防止数据遗漏
    'min_gap_days': 1,                # 最小更新间隔：距离最新数据<N天不更新
    'backup_before_update': True,     # 更新前是否备份
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
    - 波动异常：单日涨跌幅超过±5%（指数没有涨跌停，但可标记异常波动）
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
    
    # 判断波动异常（指数单日涨跌幅超过±5%视为异常）
    df['波动异常'] = df['涨跌幅'].apply(
        lambda x: 'X' if (pd.notna(x) and (abs(x) > 5)) else None
    )
    
    return df

def calculate_period_returns(df):
    """
    计算不同周期的收益率
    
    用于筛选条件13：股票相对大盘表现
    """
    df = df.copy()
    df['日期'] = pd.to_datetime(df['日期'])
    df = df.sort_values('日期')
    
    # 计算不同周期的涨跌幅
    # 1个月 ≈ 22个交易日
    # 3个月 ≈ 66个交易日
    # 6个月 ≈ 132个交易日
    
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
        logger.debug(f"{index_name}: 数据已是最新（距今{days_gap}天），跳过下载")
        return None, None, False
    
    # 计算下载范围（回溯N天防止遗漏）
    start_dt = latest_dt - timedelta(days=INCREMENTAL_CONFIG['lookback_days'])
    start_date = start_dt.strftime('%Y-%m-%d')
    
    logger.debug(f"{index_name}: 增量下载 {start_date} 至 {END_DATE}")
    
    return start_date, END_DATE, True

def download_index_data(download_code, index_name, save_code, start_date, end_date, retry_count=0):
    """
    下载单个指数的历史数据
    
    参数:
        download_code: 下载时使用的代码（如 sh.000001）
        index_name: 指数名称（如 上证指数）
        save_code: 保存时使用的代码（如 999999）
        start_date: 开始日期
        end_date: 结束日期
        retry_count: 重试次数
    """
    try:
        logger.info(f"下载 {index_name} ({start_date} 至 {end_date})")
        
        # 使用 query_history_k_data_plus 接口 - 请求完整字段
        rs = bs.query_history_k_data_plus(
            code=download_code,
            fields="date,open,high,low,close,preclose,volume,amount,pctChg",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3"  # 不复权（指数不需要复权）
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
        
        # 收集数据
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list:
            logger.warning(f"{index_name}: 无数据")
            return pd.DataFrame()
        
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
            'pctChg': '涨跌幅'
        }
        
        df = df.rename(columns=column_mapping)
        
        # 添加指数信息（使用保存代码）
        df['指数代码'] = save_code
        df['指数名称'] = index_name
        
        # 转换数据类型
        numeric_cols = ['开盘', '最高', '最低', '收盘', '昨收', '成交额', '涨跌幅']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if '成交量' in df.columns:
            df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce').astype('Int64')
        
        # 格式化日期
        df['日期'] = df['日期'].apply(format_date_string)
        
        # 删除无效行
        df = df.dropna(subset=['日期', '收盘'])
        
        if df.empty:
            logger.warning(f"{index_name}: 转换后无有效数据")
            return pd.DataFrame()
        
        # 排序
        df = df.sort_values('日期').reset_index(drop=True)
        
        # 计算派生字段
        df = calculate_derived_fields(df)
        
        logger.info(f"✅ {index_name}: 下载成功，共 {len(df)} 条记录")
        
        return df
    
    except Exception as e:
        if retry_count < MAX_RETRIES:
            logger.warning(f"{index_name}: 异常，重试 {retry_count + 1}/{MAX_RETRIES} - {e}")
            import time
            time.sleep(RETRY_DELAY)
            return download_index_data(download_code, index_name, save_code, start_date, end_date, retry_count + 1)
        else:
            logger.error(f"{index_name}: 下载异常 - {e}")
            return None

def merge_data(df_existing, df_new, index_name):
    """
    合并现有数据和新下载的数据
    
    策略：
    1. 合并两个DataFrame
    2. 按日期去重（保留新数据）
    3. 重新计算所有派生字段和周期收益
    4. 排序
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
    print("  下载大盘指数数据 v2.0 - 增量下载版（含完整字段）")
    print("  智能检测 + 增量更新 + 完整字段 + 周期收益")
    print("=" * 80)
    
    print(f"\n配置:")
    print(f"  强制全量下载: {'是' if INCREMENTAL_CONFIG['force_full_download'] else '否'}")
    print(f"  回溯天数: {INCREMENTAL_CONFIG['lookback_days']} 天")
    print(f"  最小更新间隔: {INCREMENTAL_CONFIG['min_gap_days']} 天")
    print(f"  结束日期: {END_DATE}")
    print(f"  更新前备份: {'是' if INCREMENTAL_CONFIG['backup_before_update'] else '否'}")
    
    print(f"\n✨ 完整字段支持:")
    print(f"  ✅ 基础字段：开盘、最高、最低、收盘、成交量、成交额、涨跌幅")
    print(f"  ✅ 派生字段：涨跌额、振幅、昨收、波动异常")
    print(f"  ✅ 周期收益：1月、3月、6月涨跌幅")
    
    print(f"\n指数列表:")
    for key, index_info in INDICES.items():
        print(f"  - {index_info['name']} (下载: {index_info['download_code']}, 保存: {index_info['code']})")
    
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
    print(f"\n开始增量更新指数数据...\n")
    
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
        
        # 同时保存CSV版本（可选）
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
        # 显示数据示例
        print(f"\n📊 数据示例（上证指数）:")
        sample_file = OUTPUT_DIR / "999999.parquet"
        if sample_file.exists():
            sample_df = pd.read_parquet(sample_file)
            print(f"  文件名: 999999.parquet")
            print(f"  指数代码: {sample_df['指数代码'].iloc[0]}")
            print(f"  指数名称: {sample_df['指数名称'].iloc[0]}")
            print(f"  数据行数: {len(sample_df):,}")
            print(f"  日期范围: {sample_df['日期'].min()} 至 {sample_df['日期'].max()}")
            
            print(f"\n  完整字段列表:")
            print(f"  {list(sample_df.columns)}")
            
            # 验证字段完整性
            required_fields = ['昨收', '涨跌额', '振幅', '波动异常', '涨跌幅_1月', '涨跌幅_3月', '涨跌幅_6月']
            print(f"\n  字段完整性检查:")
            for field in required_fields:
                if field in sample_df.columns:
                    valid_count = sample_df[field].notna().sum()
                    print(f"    ✅ {field}: 存在 ({valid_count}/{len(sample_df)} = {valid_count/len(sample_df)*100:.1f}%)")
                else:
                    print(f"    ❌ {field}: 缺失！")
            
            print(f"\n  最新5条记录:")
            display_cols = ['日期', '收盘', '昨收', '涨跌额', '涨跌幅', '振幅', '涨跌幅_1月', '涨跌幅_3月']
            available_cols = [col for col in display_cols if col in sample_df.columns]
            print(sample_df[available_cols].tail(5).to_string(index=False))
        
        print("\n🎉 指数数据增量更新完成！")
        print(f"💡 数据目录: {OUTPUT_DIR}")
        print(f"💡 备份目录: {BACKUP_DIR}")
        print(f"📝 日志文件: {LOG_DIR}/index_incremental_*.log")
        
        # 用途说明
        print("\n" + "=" * 80)
        print("数据用途与使用示例")
        print("=" * 80)
        print("""
✅ 已修复的功能：
   - 增量下载：只下载缺失的交易日数据
   - 完整字段：涨跌额、振幅、昨收、波动异常
   - 周期收益：1月、3月、6月涨跌幅
   - 自动备份：保留历史版本

📊 数据用途：
   1. 计算相对强度(RS Rating)
      - 个股涨幅 vs 大盘涨幅
      - 排名百分位
   
   2. 筛选条件13：相对市场表现
      - 股票1、3、6个月涨幅
      - 大幅跑赢同期大盘至少1倍以上
   
   3. 市场环境判断
      - 牛市/熊市识别
      - 行业轮动分析

💡 使用示例：
```python
import pandas as pd

# 读取上证指数（文件名为 999999.parquet）
sh = pd.read_parquet('data/index_data/999999.parquet')

# 查看完整字段
print(sh.columns.tolist())

# 查看最新数据
print(sh.tail())

# 获取最近的涨跌幅
latest_1m = sh['涨跌幅_1月'].iloc[-1]
latest_3m = sh['涨跌幅_3月'].iloc[-1]
latest_6m = sh['涨跌幅_6月'].iloc[-1]

print(f"上证指数近1月涨幅: {latest_1m:.2f}%")
print(f"上证指数近3月涨幅: {latest_3m:.2f}%")
print(f"上证指数近6月涨幅: {latest_6m:.2f}%")

# 计算相对强度
stock_return_1m = 15.5  # 假设某股票1月涨幅15.5%
relative_strength = stock_return_1m - latest_1m
print(f"相对大盘强度: {relative_strength:.2f}%")
```

⚙️ 定时更新：
   - 建议每日收盘后运行（如 18:00）
   - 强制全量下载：INCREMENTAL_CONFIG['force_full_download'] = True
   - 调整回溯天数：INCREMENTAL_CONFIG['lookback_days'] = N

⚠️ 重要说明：
   ✅ 上证指数使用 sh.000001 下载（数据完整）
   ✅ 保存为 999999.parquet 文件（统一命名）
   ✅ 指数代码字段显示为 999999（保持一致）
   ✅ 其他代码使用时，直接读取 999999.parquet 即可
        """)
    else:
        logger.error("❌ 没有成功更新任何指数数据")
        print("\n❌ 更新失败：没有成功更新任何指数数据")
        print("\n可能原因：")
        print("1. 网络连接问题")
        print("2. Baostock 服务器维护")
        print("3. 所有指数数据都已是最新")

if __name__ == "__main__":
    main()
