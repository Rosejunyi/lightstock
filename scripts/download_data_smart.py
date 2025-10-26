#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能股票数据更新脚本 V3.1
简化版：使用bash命令检查服务器数据，不依赖pandas
"""

import baostock as bs
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
import os
import sys
from tqdm import tqdm


def get_trading_days(start_date, end_date):
    """获取交易日列表"""
    lg = bs.login()
    rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
    data = []
    while rs.next():
        data.append(rs.get_row_data())
    bs.logout()
    
    df = pd.DataFrame(data, columns=rs.fields)
    trading_days = df[df['is_trading_day'] == '1']['calendar_date'].tolist()
    return trading_days


def check_server_latest_date_simple():
    """
    简化版：使用bash命令检查服务器最新文件日期
    不需要在服务器上安装pandas
    """
    server_ip = os.environ.get('SERVER_IP') or os.environ.get('ALIYUN_DB_HOST')
    server_path = os.environ.get('SERVER_PATH', '/root/lightstock')
    
    if not server_ip:
        print("⚠️ 未配置服务器地址，跳过检查")
        return None
    
    print("🔍 检查服务器数据状态...")
    print(f"服务器: {server_ip}")
    
    try:
        # 使用bash命令获取最新文件的修改时间
        check_cmd = f"""
ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no root@{server_ip} '
cd {server_path}/data/daily_parquet/ 2>/dev/null || exit 1

# 获取最新修改的文件
latest_file=$(ls -t *.parquet 2>/dev/null | head -1)

if [ -z "$latest_file" ]; then
    echo "NO_FILES"
    exit 0
fi

# 获取文件修改时间戳
file_time=$(stat -c %Y "$latest_file" 2>/dev/null || stat -f %m "$latest_file" 2>/dev/null)

# 输出：文件名|时间戳
echo "$latest_file|$file_time"
'
"""
        
        result = subprocess.run(
            check_cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            print(f"⚠️ 连接服务器失败: {result.stderr}")
            return None
        
        output = result.stdout.strip()
        
        if output == "NO_FILES":
            print("⚠️ 服务器上没有数据文件")
            return None
        
        if '|' not in output:
            print(f"⚠️ 无法解析服务器响应: {output}")
            return None
        
        filename, timestamp = output.split('|')
        file_date = datetime.fromtimestamp(int(timestamp))
        
        # 计算距今天数
        days_old = (datetime.now() - file_date).days
        
        print(f"✅ 服务器最新文件: {filename}")
        print(f"✅ 文件修改时间: {file_date.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"✅ 距今: {days_old} 天")
        
        # 如果文件是最近3天内修改的，认为数据是最新的
        if days_old <= 3:
            print("✅ 服务器数据较新，可能无需更新")
            return file_date.date()
        else:
            print(f"⚠️ 服务器数据已有 {days_old} 天，需要更新")
            return None
            
    except subprocess.TimeoutExpired:
        print("⚠️ 连接服务器超时")
        return None
    except Exception as e:
        print(f"⚠️ 检查失败: {e}")
        return None


def get_stock_list():
    """获取所有股票列表"""
    print("📋 获取股票列表...")
    lg = bs.login()
    
    rs = bs.query_all_stock(day=datetime.now().strftime("%Y-%m-%d"))
    data = []
    while rs.next():
        data.append(rs.get_row_data())
    
    bs.logout()
    
    df = pd.DataFrame(data, columns=rs.fields)
    
    # 只保留股票（排除指数）
    stocks = df[df['type'] == '1']['code'].tolist()
    print(f"✅ 获取到 {len(stocks)} 只股票")
    
    return stocks


def get_index_list():
    """获取需要的指数列表"""
    indexes = [
        "sh.000001",  # 上证指数
        "sh.000300",  # 沪深300
        "sz.399001",  # 深证成指
        "sz.399006",  # 创业板指
    ]
    return indexes


def download_stock_data(code, start_date, end_date):
    """下载单只股票数据"""
    try:
        # 提取纯数字代码
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
        while rs.next():
            data.append(rs.get_row_data())
        
        if not data:
            return None
        
        df = pd.DataFrame(data, columns=rs.fields)
        
        # 股票文件：纯数字命名
        filename = f"{pure_code}.parquet"
        
        return filename, df
        
    except Exception as e:
        print(f"  ❌ {code} 下载失败: {e}")
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
        while rs.next():
            data.append(rs.get_row_data())
        
        if not data:
            return None
        
        df = pd.DataFrame(data, columns=rs.fields)
        
        # 指数文件：保持完整格式 (如 sh.000001.parquet)
        filename = f"{code}.parquet"
        
        return filename, df
        
    except Exception as e:
        print(f"  ❌ {code} 下载失败: {e}")
        return None


def main():
    """主函数"""
    print("="*50)
    print("  智能股票数据更新 V3.1 (简化版)")
    print("="*50)
    print()
    
    # 创建输出目录
    output_dir = Path("data/daily_parquet")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # V3特性说明
    print("V3.1版本特性:")
    print("  ✅ 股票文件: 纯数字命名 (如 000001.parquet)")
    print("  ✅ 指数文件: 完整格式 (如 sh.000001.parquet)")
    print("  ✅ 简化检测: 使用bash命令，不依赖pandas")
    print("  ✅ 智能更新: 检测服务器状态，按需下载")
    print()
    
    # 检查服务器数据状态（简化版）
    server_latest = check_server_latest_date_simple()
    
    # 确定更新策略
    today = datetime.now().date()
    
    if server_latest:
        days_diff = (today - server_latest).days
        print(f"\n📊 更新策略:")
        print(f"   服务器最新: {server_latest}")
        print(f"   今天日期: {today}")
        print(f"   相差: {days_diff} 天")
        
        if days_diff <= 1:
            print("✅ 数据已是最新，跳过更新")
            print("\n💡 如需强制更新，请删除服务器上的数据文件")
            return
        else:
            # 向前推3天，确保数据完整
            start_date = (server_latest - timedelta(days=3)).strftime("%Y-%m-%d")
            print(f"⏩ 增量更新: 从 {start_date} 开始（向前推3天）")
    else:
        # 默认从3个月前开始
        start_date = (today - timedelta(days=90)).strftime("%Y-%m-%d")
        print(f"📥 首次下载或无法检测: 从 {start_date} 开始（最近3个月）")
    
    end_date = today.strftime("%Y-%m-%d")
    
    print(f"\n📅 下载区间: {start_date} ~ {end_date}")
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
        
        stocks = get_stock_list()
        success_count = 0
        
        for code in tqdm(stocks, desc="下载股票数据"):
            result = download_stock_data(code, start_date, end_date)
            if result:
                filename, df = result
                filepath = output_dir / filename
                df.to_parquet(filepath, index=False)
                success_count += 1
        
        print(f"✅ 股票数据下载完成: {success_count}/{len(stocks)}")
        print()
        
        # 下载指数数据
        print("="*50)
        print("📈 开始下载指数数据")
        print("="*50)
        
        indexes = get_index_list()
        
        for code in tqdm(indexes, desc="下载指数数据"):
            result = download_index_data(code, start_date, end_date)
            if result:
                filename, df = result
                filepath = output_dir / filename
                df.to_parquet(filepath, index=False)
                print(f"  ✅ {filename}")
        
        print("✅ 指数数据下载完成")
        print()
        
        # 统计
        total_files = len(list(output_dir.glob("*.parquet")))
        print("="*50)
        print("📊 下载完成统计")
        print("="*50)
        print(f"总文件数: {total_files}")
        print(f"输出目录: {output_dir.absolute()}")
        
    finally:
        bs.logout()
        print("\n✅ 已登出baostock")


if __name__ == "__main__":
    main()
