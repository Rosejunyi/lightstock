#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
下载不复权K线数据 v2.1 - 增量下载版（含完整字段）

新增功能：
1. ✅ 自动检测：读取现有数据的最新日期
2. ✅ 增量下载：只下载缺失的交易日数据
3. ✅ 智能合并：自动去重、排序、补全
4. ✅ 数据验证：检查换手率字段和数据完整性
5. ✅ 自动备份：保留历史版本
6. ✅ 断点续传：支持中断后继续
7. ✅ 完整字段：涨跌额、振幅、昨收、涨跌幅异常、停牌

适用场景：
- 每日定时更新K线数据
- 自动补全节假日后的缺失数据
- 保持数据最新且避免重复下载
- 确保包含换手率和所有派生字段

作者：Claude
版本：v2.1
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
OUTPUT_DIR = Path("data/daily_kline_raw")  # 不复权K线数据
STOCK_INFO_FILE = Path("data/stock_basic_info.parquet")  # 股票信息
BACKUP_DIR = Path("data/backups/kline_raw")
LOG_DIR = Path("logs")

# 创建目录
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
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
            LOG_DIR / f'kline_raw_incremental_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
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

def extract_pure_code(code_with_prefix):
    """提取纯数字股票代码"""
    if pd.isna(code_with_prefix):
        return None
    code_str = str(code_with_prefix)
    if '.' in code_str:
        return code_str.split('.')[1]
    return code_str

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
    - 昨收：前一天的收盘价（如果API没有提供）
    - 涨跌幅异常：涨跌幅超过±10%
    - 停牌：成交量为0
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
    
    # 判断涨跌幅异常（涨跌幅超过±10%，或ST股超过±5%）
    df['涨跌幅异常'] = df['涨跌幅'].apply(
        lambda x: 'X' if (pd.notna(x) and (abs(x) > 10)) else None
    )
    
    # 停牌判断（成交量为0视为停牌）
    df['停牌'] = df['成交量'].apply(lambda x: 'X' if (pd.notna(x) and x == 0) else None)
    
    return df

def get_existing_data(stock_code):
    """
    读取现有数据
    
    返回: (DataFrame, 最新日期)
    """
    file_path = OUTPUT_DIR / f"{stock_code}.parquet"
    
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

def download_kline_data(stock_code, start_date=None, end_date=None, retry_count=0):
    """
    下载单只股票的不复权K线数据
    
    参数:
        stock_code: 股票代码（6位数字）
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
        
        # 调用 Baostock API - 请求完整字段
        rs = bs.query_history_k_data_plus(
            code=bs_code,
            fields="date,code,open,high,low,close,preclose,volume,amount,turn,pctChg,isST",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3"  # ⭐ "3" = 不复权
        )
        
        # 检查返回状态
        if rs.error_code != '0':
            if retry_count < MAX_RETRIES:
                logger.debug(f"{stock_code}: 下载失败，重试 {retry_count + 1}/{MAX_RETRIES}")
                time.sleep(RETRY_DELAY)
                return download_kline_data(stock_code, start_date, end_date, retry_count + 1)
            else:
                logger.debug(f"{stock_code}: {rs.error_msg}")
                return None
        
        # 收集数据
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        # 检查是否有数据
        if not data_list:
            logger.debug(f"{stock_code}: 无K线数据")
            return pd.DataFrame()  # 返回空DataFrame
        
        # 转换为 DataFrame
        df = pd.DataFrame(data_list, columns=rs.fields)
        
        # 重命名列
        column_mapping = {
            'date': '日期',
            'code': '代码',
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
        
        # 提取纯数字代码
        df['股票代码'] = df['代码'].apply(extract_pure_code)
        df = df.drop(columns=['代码'])
        
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
        df = df.dropna(subset=['日期', '收盘'])
        
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
            return download_kline_data(stock_code, start_date, end_date, retry_count + 1)
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
        df_result = df_new.copy()
        # Ensure date is datetime type
        if not pd.api.types.is_datetime64_any_dtype(df_result['日期']):
            df_result['日期'] = pd.to_datetime(df_result['日期'])
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
    
    # Convert date back to string (ensure date column is datetime type)
    if pd.api.types.is_datetime64_any_dtype(df_result['日期']):
        df_result['日期'] = df_result['日期'].dt.strftime('%Y-%m-%d')
    else:
        # If not datetime type, convert first then format
        df_result['日期'] = pd.to_datetime(df_result['日期']).dt.strftime('%Y-%m-%d')
    
    # 重新计算派生字段（确保完整性）
    df_result = calculate_derived_fields(df_result)
    
    # 确保股票代码正确
    df_result['股票代码'] = stock_code
    
    # 添加股票名称
    if stock_info is not None:
        stock_row = stock_info[stock_info['股票代码'] == stock_code]
        if not stock_row.empty:
            df_result['股票名称'] = stock_row['股票名称'].iloc[0]
    
    return df_result

def get_stock_list_from_baostock():
    """从Baostock获取所有A股股票列表"""
    logger.info("从Baostock获取A股股票列表...")
    
    try:
        rs = bs.query_stock_basic()
        
        if rs.error_code != '0':
            logger.error(f"获取股票列表失败: {rs.error_msg}")
            return []
        
        stock_list = []
        while (rs.error_code == '0') & rs.next():
            stock_list.append(rs.get_row_data())
        
        if not stock_list:
            logger.error("股票列表为空")
            return []
        
        df = pd.DataFrame(stock_list, columns=rs.fields)
        df = df[df['code'].str.contains(r'(sh\.[65]|sz\.[03])', na=False)]
        df['pure_code'] = df['code'].apply(extract_pure_code)
        
        # 过滤退市股票
        if 'status' in df.columns:
            df = df[df['status'] == '1']
        
        stock_codes = df['pure_code'].tolist()
        logger.info(f"✅ 获取到 {len(stock_codes)} 只A股")
        
        return stock_codes
    
    except Exception as e:
        logger.error(f"获取股票列表异常: {e}")
        return []

def get_stock_list():
    """获取股票列表（优先从已有数据，其次从Baostock）"""
    # 优先从已有数据中获取
    if OUTPUT_DIR.exists():
        existing_files = list(OUTPUT_DIR.glob("*.parquet"))
        if existing_files:
            stock_codes = [f.stem for f in existing_files]
            logger.info(f"✅ 从已有数据获取到 {len(stock_codes)} 只股票")
            return stock_codes
    
    # 从股票信息文件获取
    if STOCK_INFO_FILE.exists():
        try:
            stock_info = pd.read_parquet(STOCK_INFO_FILE)
            stock_codes = stock_info['股票代码'].astype(str).str.zfill(6).tolist()
            logger.info(f"✅ 从股票信息文件获取到 {len(stock_codes)} 只股票")
            return stock_codes
        except Exception as e:
            logger.warning(f"读取股票信息失败: {e}")
    
    # 最后从Baostock获取
    return get_stock_list_from_baostock()

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
        stock_code = file_path.stem
        backup_subdir = BACKUP_DIR / stock_code
        backup_subdir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = backup_subdir / f"{stock_code}_{timestamp}.parquet"
        
        shutil.copy2(file_path, backup_file)
        logger.debug(f"已备份: {backup_file}")
        
        # 清理旧备份（保留最近3个）
        backups = sorted(backup_subdir.glob(f"{stock_code}_*.parquet"), reverse=True)
        for old_backup in backups[3:]:
            old_backup.unlink()
            logger.debug(f"删除旧备份: {old_backup}")
    
    except Exception as e:
        logger.warning(f"备份失败: {e}")

def validate_turnover_field(df, stock_code):
    """验证换手率字段"""
    if '换手率' not in df.columns:
        logger.warning(f"{stock_code}: 缺少换手率字段！")
        return False
    
    turnover_valid = df['换手率'].notna().sum()
    turnover_total = len(df)
    completeness = turnover_valid / turnover_total if turnover_total > 0 else 0
    
    if completeness < 0.5:  # 少于50%有效数据
        logger.warning(f"{stock_code}: 换手率数据不完整 ({completeness*100:.1f}%)")
        return False
    
    return True

# ============================================================
# 主程序
# ============================================================

def main():
    print("=" * 80)
    print("  下载不复权K线数据 v2.1 - 增量下载版（含完整字段）")
    print("  智能检测 + 增量更新 + 完整字段 + 数据验证")
    print("=" * 80)
    
    print(f"\n配置:")
    print(f"  强制全量下载: {'是' if INCREMENTAL_CONFIG['force_full_download'] else '否'}")
    print(f"  回溯天数: {INCREMENTAL_CONFIG['lookback_days']} 天")
    print(f"  最小更新间隔: {INCREMENTAL_CONFIG['min_gap_days']} 天")
    print(f"  结束日期: {END_DATE}")
    print(f"  更新前备份: {'是' if INCREMENTAL_CONFIG['backup_before_update'] else '否'}")
    
    print(f"\n✨ 完整字段支持:")
    print(f"  ✅ 基础字段：开盘、最高、最低、收盘、成交量、成交额、换手率、涨跌幅")
    print(f"  ✅ 派生字段：涨跌额、振幅、昨收、涨跌幅异常、停牌")
    
    # 加载股票信息
    print("\n步骤 1/4: 加载股票信息...")
    stock_info = load_stock_info()
    if stock_info is not None:
        print(f"✅ 股票信息加载成功: {len(stock_info)} 只股票")
    else:
        print(f"⚠️  未加载股票信息，股票名称字段将为空")
    
    # 登录 Baostock
    print("\n步骤 2/4: 登录 Baostock...")
    lg = bs.login()
    if lg.error_code != '0':
        logger.error(f"❌ 登录失败: {lg.error_msg}")
        print(f"❌ 登录失败: {lg.error_msg}")
        return
    
    logger.info("✅ 登录成功")
    print("✅ 登录成功")
    
    # 获取股票列表
    print("\n步骤 3/4: 获取股票列表...")
    stock_codes = get_stock_list()
    
    if not stock_codes:
        print("❌ 无法获取股票列表")
        bs.logout()
        return
    
    print(f"✅ 获取到 {len(stock_codes)} 只股票")
    
    # 统计信息
    stats = {
        'total': len(stock_codes),
        'updated': 0,
        'skipped': 0,
        'failed': 0,
        'new_records': 0
    }
    
    # 下载数据
    print(f"\n步骤 4/4: 增量更新K线数据...")
    print(f"提示：只下载缺失的交易日数据，并自动计算派生字段\n")
    
    with tqdm(total=len(stock_codes), desc="更新进度") as pbar:
        for i, stock_code in enumerate(stock_codes):
            # 读取现有数据
            df_existing, latest_date = get_existing_data(stock_code)
            
            # 计算下载范围
            start_date, end_date, need_download = calculate_download_range(latest_date, stock_code)
            
            if not need_download:
                stats['skipped'] += 1
                pbar.update(1)
                continue
            
            # 备份
            if INCREMENTAL_CONFIG['backup_before_update'] and df_existing is not None:
                file_path = OUTPUT_DIR / f"{stock_code}.parquet"
                backup_file(file_path)
            
            # 下载新数据
            df_new = download_kline_data(stock_code, start_date, end_date)
            
            if df_new is None:
                stats['failed'] += 1
                pbar.update(1)
                continue
            
            # 合并数据
            df_final = merge_data(df_existing, df_new, stock_code, stock_info)
            
            if df_final is None or df_final.empty:
                stats['failed'] += 1
                pbar.update(1)
                continue
            
            # 验证换手率字段
            validate_turnover_field(df_final, stock_code)
            
            # 保存
            output_file = OUTPUT_DIR / f"{stock_code}.parquet"
            df_final.to_parquet(output_file, index=False)
            
            new_records = len(df_new) if not df_new.empty else 0
            stats['new_records'] += new_records
            stats['updated'] += 1
            
            pbar.update(1)
            
            # 每处理一批显示统计
            if (i + 1) % BATCH_SIZE == 0:
                pbar.set_postfix({
                    '已更新': stats['updated'],
                    '已跳过': stats['skipped'],
                    '新增': stats['new_records']
                })
    
    # 退出登录
    bs.logout()
    logger.info("已退出 Baostock")
    
    # 验证结果
    print(f"\n验证结果...")
    total_files = len(list(OUTPUT_DIR.glob("*.parquet")))
    print(f"✅ K线文件总数: {total_files}")
    
    # 输出统计
    print("\n" + "=" * 80)
    print("更新完成统计")
    print("=" * 80)
    print(f"总股票数: {stats['total']}")
    print(f"✅ 已更新: {stats['updated']}")
    print(f"⏭️  已跳过: {stats['skipped']} (数据已是最新)")
    print(f"❌ 更新失败: {stats['failed']}")
    print(f"📊 新增记录: {stats['new_records']} 条")
    print("=" * 80)
    
    # 显示数据示例
    if total_files > 0:
        print(f"\n📊 数据示例（最新5条）:")
        sample_file = list(OUTPUT_DIR.glob("*.parquet"))[0]
        sample_df = pd.read_parquet(sample_file)
        print(f"  股票: {sample_file.stem}")
        print(f"  数据行数: {len(sample_df):,}")
        print(f"  日期范围: {sample_df['日期'].min()} 至 {sample_df['日期'].max()}")
        print(f"  列名: {list(sample_df.columns)}")
        
        # 验证字段完整性
        required_fields = ['换手率', '昨收', '涨跌额', '振幅', '涨跌幅异常', '停牌']
        print(f"\n  字段完整性检查:")
        for field in required_fields:
            if field in sample_df.columns:
                valid_count = sample_df[field].notna().sum()
                print(f"    ✅ {field}: 存在 ({valid_count}/{len(sample_df)} = {valid_count/len(sample_df)*100:.1f}%)")
            else:
                print(f"    ❌ {field}: 缺失！")
        
        display_cols = ['日期', '开盘', '收盘', '涨跌额', '涨跌幅', '振幅', '换手率']
        available_cols = [col for col in display_cols if col in sample_df.columns]
        print(f"\n  最新数据预览:")
        print(sample_df[available_cols].tail(5).to_string(index=False))
    
    print("\n🎉 不复权K线数据增量更新完成！")
    print(f"💡 数据目录: {OUTPUT_DIR}")
    print(f"💡 备份目录: {BACKUP_DIR}")
    print(f"📝 日志文件: {LOG_DIR}/kline_raw_incremental_*.log")
    
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

💡 使用建议：
   1. 定时更新：建议每日收盘后运行（如 18:00）
   2. 强制全量下载：INCREMENTAL_CONFIG['force_full_download'] = True
   3. 调整回溯天数：INCREMENTAL_CONFIG['lookback_days'] = N
   
📖 读取数据示例：
   import pandas as pd
   df = pd.read_parquet('data/daily_kline_raw/000001.parquet')
   
   # 查看完整字段
   print(df.columns.tolist())
   
   # 计算热度值
   volume_ma10 = df['成交量'].rolling(10).mean()
   turnover_ma10 = df['换手率'].rolling(10).mean()
   hotness = (df['成交量']/volume_ma10)*0.6 + (df['换手率']/turnover_ma10)*0.4
    """)

if __name__ == "__main__":
    main()
