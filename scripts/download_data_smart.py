#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能股票数据更新脚本 V3.4 - 简化版
策略：固定下载最近3天，自动去重合并
"""

import baostock as bs
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from tqdm import tqdm


# ==========================================
# 🔧 调试开关（测试完成后改为 False）
# ==========================================
DEBUG_MODE = True  # True=只处理5只股票，False=处理全部股票
DEBUG_STOCK_COUNT = 5  # 调试模式下处理的股票数量
# ==========================================


def get_stock_list():
    """获取所有股票列表（增强调试版）"""
    print("📋 获取股票列表...")
    
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        print(f"查询日期: {today}")
        
        rs = bs.query_all_stock(day=today)
        
        if rs.error_code != '0':
            print(f"❌ query_all_stock 查询失败")
            print(f"   错误码: {rs.error_code}")
            print(f"   错误信息: {rs.error_msg}")
            print("\n使用备用方法...")
            return get_stock_list_fallback()
        
        data = []
        count = 0
        while (rs.error_code == '0') and rs.next():
            data.append(rs.get_row_data())
            count += 1
        
        print(f"📊 从baostock获取到 {len(data)} 条记录")
        
        if len(data) < 100:
            print(f"⚠️ 获取的记录数太少（{len(data)}条），使用备用方法")
            return get_stock_list_fallback()
        
        df = pd.DataFrame(data, columns=rs.fields)
        
        print(f"📋 列名: {list(df.columns)}")
        
        if 'type' in df.columns:
            print("✅ 找到 'type' 列")
            print(f"   type 列的唯一值: {df['type'].unique()}")
            stocks = df[df['type'] == '1']['code'].tolist()
            print(f"✅ 使用 type 过滤，获取到 {len(stocks)} 只股票")
        else:
            print("⚠️ 没有 'type' 列，使用代码格式过滤")
            stocks = filter_stocks_by_code(df)
        
        if stocks and len(stocks) > 0:
            print(f"📋 示例股票代码（前10个）: {stocks[:10]}")
            return stocks
        else:
            print("❌ 过滤后没有股票，使用备用方法")
            return get_stock_list_fallback()
        
    except Exception as e:
        print(f"❌ 获取股票列表异常: {e}")
        import traceback
        traceback.print_exc()
        print("\n使用备用方法...")
        return get_stock_list_fallback()


def filter_stocks_by_code(df):
    """通过代码格式过滤股票"""
    all_codes = df['code'].tolist()
    
    exclude_list = [
        'sh.000001', 'sh.000300', 
        'sz.399001', 'sz.399006', 'sz.399005', 'sz.399300'
    ]
    
    stocks = []
    for code in all_codes:
        if code in exclude_list:
            continue
        
        code_num = code.split('.')[-1]
        
        if len(code_num) == 6 and code_num.isdigit():
            if not code_num.startswith('399'):
                stocks.append(code)
    
    print(f"✅ 代码格式过滤，获取到 {len(stocks)} 只股票")
    return stocks


def get_stock_list_fallback():
    """备用方法：生成股票代码范围"""
    print("📋 使用备用方法：生成股票代码...")
    
    stocks = []
    
    print("  生成沪市A股...")
    for prefix in ['600', '601', '603', '605']:
        for i in range(1000):
            stocks.append(f"sh.{prefix}{i:03d}")
    
    print("  生成深市主板...")
    for i in range(2, 1000):
        stocks.append(f"sz.{i:06d}")
    
    print("  生成中小板...")
    for i in range(2000, 3000):
        stocks.append(f"sz.{i:06d}")
    
    print("  生成创业板...")
    for i in range(300000, 301000):
        stocks.append(f"sz.{i}")
    
    print("  生成科创板...")
    for i in range(688000, 689000):
        stocks.append(f"sh.{i}")
    
    print(f"✅ 备用方法生成了 {len(stocks)} 个代码")
    print(f"📋 示例: {stocks[:10]}")
    
    return stocks


def get_index_list():
    """获取需要的指数列表"""
    return ["sh.000001", "sh.000300", "sz.399001", "sz.399006"]


def download_stock_data(code, start_date, end_date):
    """下载单只股票数据"""
    try:
        pure_code = code.split('.')[-1]
        
        rs = bs.query_history_k_data_plus(
            code,
            "date,code,open,high,low,close,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3"
        )
        
        data = []
        while (rs.error_code == '0') and rs.next():
            data.append(rs.get_row_data())
        
        if not data:
            return None
        
        df = pd.DataFrame(data, columns=rs.fields)
        filename = f"{pure_code}.parquet"
        
        return filename, df
        
    except Exception as e:
        return None


def download_index_data(code, start_date, end_date):
    """下载指数数据"""
    try:
        rs = bs.query_history_k_data_plus(
            code,
            "date,code,open,high,low,close,volume,amount",
            start_date=start_date,
            end_date=end_date,
            frequency="d"
        )
        
        data = []
        while (rs.error_code == '0') and rs.next():
            data.append(rs.get_row_data())
        
        if not data:
            return None
        
        df = pd.DataFrame(data, columns=rs.fields)
        filename = f"{code}.parquet"
        
        return filename, df
        
    except Exception as e:
        return None


def main():
    """主函数"""
    print("="*50)
    if DEBUG_MODE:
        print(f"  智能股票数据更新 V3.4 [🔧 调试模式]")
    else:
        print("  智能股票数据更新 V3.4 [正式模式]")
    print("="*50)
    print()
    
    # 创建输出目录
    output_dir = Path("data/daily_parquet")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("V3.4 更新策略:")
    print("  ✅ 固定下载最近3天数据")
    print("  ✅ 自动去重合并")
    print("  ✅ 不检查服务器（简化逻辑）")
    if DEBUG_MODE:
        print(f"  🔧 调试模式：仅处理 {DEBUG_STOCK_COUNT} 只股票")
    print()
    
    # 固定下载最近3天
    today = datetime.now().date()
    start_date = (today - timedelta(days=3)).strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")
    
    print(f"📅 下载区间: {start_date} ~ {end_date}")
    print(f"   （固定最近3天，包含今天）")
    print()
    
    # 登录
    print("🔐 登录baostock...")
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ 登录失败: {lg.error_msg}")
        return
    print("✅ 登录成功")
    print()
    
    try:
        # 下载股票数据
        print("="*50)
        print("📊 开始下载股票数据")
        print("="*50)
        print()
        
        stocks = get_stock_list()
        
        if not stocks or len(stocks) == 0:
            print("❌ 未获取到股票列表，程序终止")
            return
        
        # ==========================================
        # 🔧 调试模式：限制股票数量
        # ==========================================
        if DEBUG_MODE:
            original_count = len(stocks)
            stocks = stocks[:DEBUG_STOCK_COUNT]
            print(f"🔧 [调试模式] 从 {original_count} 只股票中选取前 {len(stocks)} 只")
            print(f"   调试股票: {stocks}")
            print()
        # ==========================================
        
        print(f"开始下载 {len(stocks)} 只股票...")
        print()
        
        success_count = 0
        skip_count = 0
        
        for code in tqdm(stocks, desc="下载股票"):
            result = download_stock_data(code, start_date, end_date)
            if result:
                filename, df = result
                filepath = output_dir / filename
                
                # 如果文件存在，合并数据
                if filepath.exists():
                    try:
                        old_df = pd.read_parquet(filepath)
                        df = pd.concat([old_df, df], ignore_index=True)
                        df.drop_duplicates(subset=['date'], keep='last', inplace=True)
                        df.sort_values('date', inplace=True)
                    except:
                        pass
                
                df.to_parquet(filepath, index=False)
                success_count += 1
            else:
                skip_count += 1
        
        print(f"\n✅ 股票数据下载完成:")
        print(f"   成功: {success_count}")
        print(f"   跳过/无数据: {skip_count}")
        print()
        
        # 下载指数数据
        print("="*50)
        print("📈 开始下载指数数据")
        print("="*50)
        print()
        
        indexes = get_index_list()
        
        for code in tqdm(indexes, desc="下载指数"):
            result = download_index_data(code, start_date, end_date)
            if result:
                filename, df = result
                filepath = output_dir / filename
                
                # 如果文件存在，合并数据
                if filepath.exists():
                    try:
                        old_df = pd.read_parquet(filepath)
                        df = pd.concat([old_df, df], ignore_index=True)
                        df.drop_duplicates(subset=['date'], keep='last', inplace=True)
                        df.sort_values('date', inplace=True)
                    except:
                        pass
                
                df.to_parquet(filepath, index=False)
                print(f"  ✅ {filename}")
        
        print("\n✅ 指数数据下载完成")
        print()
        
        # 统计
        total_files = len(list(output_dir.glob("*.parquet")))
        total_size = sum(f.stat().st_size for f in output_dir.glob("*.parquet")) / 1024 / 1024
        
        print("="*50)
        print("📊 下载完成统计")
        print("="*50)
        print(f"总文件数: {total_files}")
        print(f"总大小: {total_size:.2f} MB")
        print(f"输出目录: {output_dir.absolute()}")
        
        if DEBUG_MODE:
            print()
            print("🔧 调试模式提示:")
            print("   测试完成后，请修改脚本顶部:")
            print("   DEBUG_MODE = False  # 改为 False 启用全量下载")
        
    except Exception as e:
        print(f"\n❌ 下载过程出错: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        bs.logout()
        print("\n✅ 已登出baostock")


if __name__ == "__main__":
    main()
