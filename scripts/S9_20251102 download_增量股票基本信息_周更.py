#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
下载股票基本信息 v4.0 - 增量更新版

新增功能：
1. ✅ 智能检测：检查现有数据的更新时间
2. ✅ 增量更新：只更新新增/变化的股票
3. ✅ 历史备份：保留上一版本数据
4. ✅ 对比报告：显示新增、退市、变化的股票

适用场景：
- 定期维护股票基本信息（建议每月运行一次）
- 自动跳过不需要更新的情况
- 保留历史版本便于追溯

作者：Claude
版本：v4.0
日期：2025-11-02
"""

import baostock as bs
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from tqdm import tqdm
import logging
import time
import shutil

# 尝试导入 akshare
try:
    import akshare as ak
    HAS_AKSHARE = True
except ImportError:
    HAS_AKSHARE = False

# ============================================================
# 配置
# ============================================================

OUTPUT_FILE = Path("data/stock_basic_info.parquet")
BACKUP_DIR = Path("data/backups/stock_basic_info")
LOG_DIR = Path("logs")

# 更新策略配置
UPDATE_CONFIG = {
    'force_update': False,           # 是否强制更新（忽略时间检查）
    'update_interval_days': 7,       # 更新间隔（天）：距离上次更新超过N天才更新
    'keep_backups': 5,               # 保留最近N个备份
}

MAX_RETRIES = 3
RETRY_DELAY = 2

# 创建目录
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(
            LOG_DIR / f'stock_info_incremental_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
            encoding='utf-8'
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# 工具函数
# ============================================================

def check_need_update():
    """
    检查是否需要更新
    
    返回: (需要更新, 原因, 现有数据)
    """
    if UPDATE_CONFIG['force_update']:
        return True, "强制更新模式", None
    
    if not OUTPUT_FILE.exists():
        return True, "首次运行，无历史数据", None
    
    try:
        # 读取现有数据
        df_existing = pd.read_parquet(OUTPUT_FILE)
        
        if df_existing.empty:
            return True, "现有数据为空", None
        
        # 检查更新时间
        if '更新时间' in df_existing.columns:
            last_update = df_existing['更新时间'].iloc[0]
            last_update_dt = pd.to_datetime(last_update)
            days_since_update = (datetime.now() - last_update_dt).days
            
            if days_since_update < UPDATE_CONFIG['update_interval_days']:
                return False, f"距离上次更新仅{days_since_update}天，无需更新", df_existing
            else:
                return True, f"距离上次更新已{days_since_update}天", df_existing
        else:
            return True, "无更新时间信息", df_existing
    
    except Exception as e:
        logger.warning(f"读取现有数据失败: {e}")
        return True, "读取现有数据失败", None

def backup_existing_data():
    """备份现有数据"""
    if not OUTPUT_FILE.exists():
        return None
    
    try:
        # 生成备份文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = BACKUP_DIR / f"stock_basic_info_{timestamp}.parquet"
        
        # 复制文件
        shutil.copy2(OUTPUT_FILE, backup_file)
        logger.info(f"✅ 已备份到: {backup_file}")
        
        # 清理旧备份
        cleanup_old_backups()
        
        return backup_file
    
    except Exception as e:
        logger.error(f"备份失败: {e}")
        return None

def cleanup_old_backups():
    """清理旧备份，只保留最近N个"""
    try:
        backups = sorted(BACKUP_DIR.glob("stock_basic_info_*.parquet"), reverse=True)
        
        if len(backups) > UPDATE_CONFIG['keep_backups']:
            for old_backup in backups[UPDATE_CONFIG['keep_backups']:]:
                old_backup.unlink()
                logger.info(f"删除旧备份: {old_backup.name}")
    
    except Exception as e:
        logger.warning(f"清理旧备份失败: {e}")

def get_stock_list():
    """获取所有股票列表"""
    logger.info("获取股票列表...")
    
    all_stocks = []
    rs = bs.query_stock_basic()
    if rs.error_code == '0':
        while (rs.error_code == '0') & rs.next():
            all_stocks.append(rs.get_row_data())
    
    if not all_stocks:
        logger.error("❌ 获取股票列表失败")
        return None
    
    df = pd.DataFrame(all_stocks, columns=rs.fields)
    df = df[df['code'].str.startswith(('sh.6', 'sz.0', 'sz.3'))]
    logger.info(f"✅ 获取到 {len(df)} 只股票")
    
    return df

def get_stock_industry(stock_code, retry_count=0):
    """获取股票的行业分类"""
    try:
        rs = bs.query_stock_industry(code=stock_code)
        
        if rs.error_code != '0':
            if retry_count < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
                return get_stock_industry(stock_code, retry_count + 1)
            return None
        
        industry_list = []
        while (rs.error_code == '0') & rs.next():
            row_data = rs.get_row_data()
            industry_list.append(row_data)
        
        if not industry_list:
            return None
        
        return industry_list[0]
    
    except Exception as e:
        logger.warning(f"{stock_code}: 获取行业失败 - {e}")
        return None

def get_all_stock_shares_akshare():
    """使用 AKShare 批量获取所有股票的股本数据"""
    if not HAS_AKSHARE:
        logger.warning("未安装 akshare，跳过股本数据获取")
        return {}
    
    try:
        logger.info("正在从 AKShare 批量获取股本数据...")
        
        df_spot = ak.stock_zh_a_spot_em()
        
        shares_dict = {}
        for _, row in df_spot.iterrows():
            code = row['代码']
            price = row.get('最新价', None)
            total_mv = row.get('总市值', None)
            float_mv = row.get('流通市值', None)
            
            if price and total_mv and price > 0:
                total_shares = total_mv / price / 10000
                float_shares = float_mv / price / 10000 if float_mv and float_mv > 0 else None
                
                shares_dict[code] = {
                    '总股本': round(total_shares, 2),
                    '流通股本': round(float_shares, 2) if float_shares else None
                }
        
        logger.info(f"✅ 获取到 {len(shares_dict)} 只股票的股本数据")
        return shares_dict
    
    except Exception as e:
        logger.error(f"从 AKShare 获取股本数据失败: {e}")
        return {}

def compare_with_existing(df_new, df_old):
    """
    对比新旧数据，生成变化报告
    
    返回: 新增股票, 退市股票, 变化股票
    """
    if df_old is None or df_old.empty:
        return df_new, pd.DataFrame(), pd.DataFrame()
    
    # 获取股票代码集合
    codes_new = set(df_new['股票代码'])
    codes_old = set(df_old['股票代码'])
    
    # 新增股票
    new_codes = codes_new - codes_old
    df_new_stocks = df_new[df_new['股票代码'].isin(new_codes)]
    
    # 退市股票
    delisted_codes = codes_old - codes_new
    df_delisted = df_old[df_old['股票代码'].isin(delisted_codes)]
    
    # 变化股票（行业或其他信息变化）
    common_codes = codes_new & codes_old
    changes = []
    
    for code in common_codes:
        row_new = df_new[df_new['股票代码'] == code].iloc[0]
        row_old = df_old[df_old['股票代码'] == code].iloc[0]
        
        # 检查关键字段是否变化
        if row_new['行业'] != row_old['行业']:
            changes.append({
                '股票代码': code,
                '股票名称': row_new['股票名称'],
                '变化类型': '行业变更',
                '旧值': row_old['行业'],
                '新值': row_new['行业']
            })
    
    df_changes = pd.DataFrame(changes)
    
    return df_new_stocks, df_delisted, df_changes

def incremental_update(df_new, df_old):
    """
    增量更新：合并新旧数据
    
    策略：
    1. 保留旧数据中的历史记录
    2. 更新共同股票的信息
    3. 添加新增股票
    4. 标记退市股票（但不删除）
    """
    if df_old is None or df_old.empty:
        return df_new
    
    # 获取股票代码集合
    codes_new = set(df_new['股票代码'])
    codes_old = set(df_old['股票代码'])
    
    # 1. 更新共同股票（使用新数据）
    common_codes = codes_new & codes_old
    df_updated = df_new[df_new['股票代码'].isin(common_codes)]
    
    # 2. 添加新增股票
    new_codes = codes_new - codes_old
    df_new_stocks = df_new[df_new['股票代码'].isin(new_codes)]
    
    # 3. 保留退市股票（标记）
    delisted_codes = codes_old - codes_new
    df_delisted = df_old[df_old['股票代码'].isin(delisted_codes)].copy()
    if not df_delisted.empty:
        df_delisted['状态'] = '已退市'
        df_delisted['更新时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 合并
    df_result = pd.concat([df_updated, df_new_stocks, df_delisted], ignore_index=True)
    
    # 排序
    df_result = df_result.sort_values('股票代码').reset_index(drop=True)
    
    return df_result

# ============================================================
# 主程序
# ============================================================

def main():
    print("=" * 80)
    print("  下载股票基本信息 v4.0 - 增量更新版")
    print("  智能检测 + 自动备份 + 变化报告")
    print("=" * 80)
    
    # 检查是否需要更新
    need_update, reason, df_existing = check_need_update()
    
    print(f"\n📊 更新检查:")
    print(f"  状态: {'需要更新' if need_update else '无需更新'}")
    print(f"  原因: {reason}")
    
    if df_existing is not None:
        last_update = df_existing['更新时间'].iloc[0] if '更新时间' in df_existing.columns else '未知'
        print(f"  上次更新: {last_update}")
        print(f"  现有股票数: {len(df_existing)}")
    
    if not need_update:
        print(f"\n💡 提示:")
        print(f"  - 如需强制更新，请修改配置：UPDATE_CONFIG['force_update'] = True")
        print(f"  - 或修改更新间隔：UPDATE_CONFIG['update_interval_days'] = N")
        return
    
    # 用户确认
    print(f"\n⚠️  即将开始更新...")
    if df_existing is not None:
        response = input("是否继续？(y/n): ")
        if response.lower() != 'y':
            print("已取消更新")
            return
    
    # 备份现有数据
    if df_existing is not None:
        print(f"\n📦 备份现有数据...")
        backup_file = backup_existing_data()
        if backup_file:
            print(f"  ✅ 备份成功")
    
    # 检查 AKShare
    if not HAS_AKSHARE:
        print("\n⚠️  未安装 akshare，将无法获取股本数据")
    
    # 登录 Baostock
    print("\n登录 Baostock...")
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ 登录失败: {lg.error_msg}")
        return
    
    print("✅ 登录成功")
    
    # 获取股票列表
    stock_list_df = get_stock_list()
    if stock_list_df is None or stock_list_df.empty:
        print("❌ 获取股票列表失败")
        bs.logout()
        return
    
    # 批量获取股本数据
    shares_dict = get_all_stock_shares_akshare() if HAS_AKSHARE else {}
    
    # 收集股票信息
    stock_info_list = []
    
    print("\n开始整理股票基本信息...\n")
    
    for idx, row in tqdm(stock_list_df.iterrows(), total=len(stock_list_df), desc="处理进度"):
        stock_code_full = row['code']
        stock_code_pure = stock_code_full.split('.')[1]
        stock_name = row.get('code_name', '')
        
        # 基本信息
        info = {
            '股票代码': stock_code_pure,
            '股票名称': stock_name,
            '市场': stock_code_full.split('.')[0],
            '上市日期': row.get('ipoDate', ''),
            '退市日期': row.get('outDate', ''),
            '股票类型': row.get('type', ''),
            '状态': row.get('status', ''),
        }
        
        # 获取行业信息
        industry_info = get_stock_industry(stock_code_full)
        if industry_info and len(industry_info) >= 5:
            info['行业'] = industry_info[3] if industry_info[3] else ''
            info['行业分类'] = industry_info[4] if industry_info[4] else ''
        else:
            info['行业'] = ''
            info['行业分类'] = ''
        
        # 获取股本信息
        if shares_dict and stock_code_pure in shares_dict:
            info['总股本'] = shares_dict[stock_code_pure]['总股本']
            info['流通股本'] = shares_dict[stock_code_pure]['流通股本']
        else:
            info['总股本'] = None
            info['流通股本'] = None
        
        # 添加更新时间
        info['更新时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        stock_info_list.append(info)
        
        if (idx + 1) % 100 == 0:
            time.sleep(0.5)
    
    bs.logout()
    
    # 转换为 DataFrame
    df_new = pd.DataFrame(stock_info_list)
    
    # 数据类型转换
    for col in ['总股本', '流通股本']:
        if col in df_new.columns:
            df_new[col] = pd.to_numeric(df_new[col], errors='coerce')
    
    # 对比分析
    if df_existing is not None:
        print("\n" + "=" * 80)
        print("变化分析")
        print("=" * 80)
        
        df_new_stocks, df_delisted, df_changes = compare_with_existing(df_new, df_existing)
        
        print(f"📈 新增股票: {len(df_new_stocks)} 只")
        if not df_new_stocks.empty:
            print("\n新增股票列表:")
            for _, row in df_new_stocks.head(10).iterrows():
                print(f"  - {row['股票代码']} {row['股票名称']} ({row['行业']})")
            if len(df_new_stocks) > 10:
                print(f"  ... 还有 {len(df_new_stocks) - 10} 只")
        
        print(f"\n📉 退市股票: {len(df_delisted)} 只")
        if not df_delisted.empty:
            print("\n退市股票列表:")
            for _, row in df_delisted.head(10).iterrows():
                print(f"  - {row['股票代码']} {row['股票名称']}")
        
        print(f"\n🔄 信息变化: {len(df_changes)} 只")
        if not df_changes.empty:
            print("\n变化详情:")
            for _, row in df_changes.head(10).iterrows():
                print(f"  - {row['股票代码']} {row['股票名称']}: {row['变化类型']}")
                print(f"    {row['旧值']} → {row['新值']}")
        
        # 增量更新
        df_final = incremental_update(df_new, df_existing)
        print(f"\n✅ 增量更新完成:")
        print(f"  原有: {len(df_existing)} 只")
        print(f"  更新后: {len(df_final)} 只")
    else:
        df_final = df_new
        print(f"\n✅ 首次下载完成: {len(df_final)} 只")
    
    # 保存
    df_final.to_parquet(OUTPUT_FILE, index=False)
    csv_file = OUTPUT_FILE.parent / 'stock_basic_info.csv'
    df_final.to_csv(csv_file, index=False, encoding='utf-8-sig')
    
    # 输出统计
    print("\n" + "=" * 80)
    print("✅ 更新完成")
    print("=" * 80)
    print(f"总股票数: {len(df_final)}")
    print(f"有行业信息: {df_final['行业'].notna().sum()} ({df_final['行业'].notna().sum()/len(df_final)*100:.1f}%)")
    print(f"有总股本: {df_final['总股本'].notna().sum()} ({df_final['总股本'].notna().sum()/len(df_final)*100:.1f}%)")
    print(f"有流通股本: {df_final['流通股本'].notna().sum()} ({df_final['流通股本'].notna().sum()/len(df_final)*100:.1f}%)")
    print("=" * 80)
    
    # 显示数据示例
    print("\n📊 数据示例:")
    display_cols = ['股票代码', '股票名称', '行业', '总股本', '流通股本', '状态']
    available_cols = [col for col in display_cols if col in df_final.columns]
    print(df_final[available_cols].head(10).to_string(index=False))
    
    print(f"\n💡 数据已保存至: {OUTPUT_FILE}")
    print(f"💡 备份目录: {BACKUP_DIR}")

if __name__ == "__main__":
    main()
