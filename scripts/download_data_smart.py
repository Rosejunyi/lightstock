#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能股票数据更新脚本 V3.3 - 完整修正版
- 修正增量更新逻辑（去除3天回溯）
- 修正股票列表获取（增强调试）
"""

import baostock as bs
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import subprocess
import os
from tqdm import tqdm


def check_server_latest_date():
    """检查服务器最新文件的修改日期"""
    server_ip = os.environ.get('SERVER_IP')
    server_path = os.environ.get('SERVER_PATH', '/root/lightstock')
    
    if not server_ip:
        print("⚠️ 未配置服务器地址，跳过检查")
        return None
    
    print("🔍 检查服务器数据状态...")
    print(f"服务器: {server_ip}")
    
    try:
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
            print(f"⚠️ 连接服务器失败")
            return None
        
        output = result.stdout.strip()
        
        if output == "NO_FILES":
            print("⚠️ 服务器上没有数据文件")
            return None
        
        if '|' not in output:
            print(f"⚠️ 无法解析服务器响应")
            return None
        
        filename, timestamp = output.split('|')
        file_date = datetime.fromtimestamp(int(timestamp))
        
        days_old = (datetime.now() - file_date).days
        
        print(f"✅ 服务器最新文件: {filename}")
        print(f"✅ 文件修改时间: {file_date.strftime('%Y-%m-%d')}")
        print(f"✅ 距今: {days_old} 天")
        
        return file_date.date()
            
    except Exception as e:
        print(f"⚠️ 检查失败: {e}")
        return None


def get_stock_list():
    """获取所有股票列表（增强调试版）"""
    print("📋 获取股票列表...")
    
    try:
        # 获取当前日期
        today = datetime.now().strftime("%Y-%m-%d")
        print(f"查询日期: {today}")
        
        # 查询所有股票
        rs = bs.query_all_stock(day=today)
        
        # 检查查询结果
        if rs.error_code != '0':
            print(f"❌ query_all_stock 查询失败")
            print(f"   错误码: {rs.error_code}")
            print(f"   错误信息: {rs.error_msg}")
            print("\n尝试使用备用方法...")
            return get_stock_list_fallback()
        
        # 获取数据
        data = []
        count = 0
        while (rs.error_code == '0') and rs.next():
            data.append(rs.get_row_data())
            count += 1
        
        print(f"📊 从baostock获取到 {len(data)} 条记录（循环 {count} 次）")
        
        if len(data) < 100:
            print(f"⚠️ 获取的记录数太少（{len(data)}条），使用备用方法")
            return get_stock_list_fallback()
        
        # 创建DataFrame
        df = pd.DataFrame(data, columns=rs.fields)
        
        # 打印调试信息
        print(f"📋 列名: {list(df.columns)}")
        print(f"📋 数据示例（前3行）:")
        print(df.head(3))
        
        # 检查是否有 type 列
        if 'type' in df.columns:
            print("✅ 找到 'type' 列")
            # 打印 type 列的唯一值
            print(f"   type 列的唯一值: {df['type'].unique()}")
            
            # 1 = 股票, 2 = 指数
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
        print("\n尝试使用备用方法...")
        return get_stock_list_fallback()


def filter_stocks_by_code(df):
    """通过代码格式过滤股票"""
    all_codes = df['code'].tolist()
    
    # 排除已知指数
    exclude_list = [
        'sh.000001', 'sh.000300', 
        'sz.399001', 'sz.399006', 'sz.399005', 'sz.399300'
    ]
    
    stocks = []
    for code in all_codes:
        if code in exclude_list:
            continue
        
        # 提取代码数字部分
        code_num = code.split('.')[-1]
        
        # 股票代码必须是6位数字，且不以399开头（深证指数）
        if len(code_num) == 6 and code_num.isdigit():
            if not code_num.startswith('399'):
                stocks.append(code)
    
    print(f"✅ 代码格式过滤，获取到 {len(stocks)} 只股票")
    return stocks


def get_stock_list_fallback():
    """备用方法：生成股票代码范围"""
    print("📋 使用备用方法：生成股票代码...")
    
    stocks = []
    
    # 沪市A股：600000-605000, 601000-605000（常见范围）
    print("  生成沪市A股...")
    for prefix in ['600', '601', '603', '605']:
        for i in range(1000):
            stocks.append(f"sh.{prefix}{i:03d}")
    
    # 深市主板：000002-000999（排除000001指数）
    print("  生成深市主板...")
    for i in range(2, 1000):
        stocks.append(f"sz.{i:06d}")
    
    # 中小板：002000-002999
    print("  生成中小板...")
    for i in range(2000, 3000):
        stocks.append(f"sz.{i:06d}")
    
    # 创业板：300000-300999
    print("  生成创业板...")
    for i in range(300000, 301000):
        stocks.append(f"sz.{i}")
    
    # 科创板：688000-688999
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
    print("  智能股票数据更新 V3.3")
    print("="*50)
    print()
    
    # 创建输出目录
    output_dir = Path("data/daily_parquet")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("V3.3 更新策略:")
    print("  ✅ 股票文件: 纯数字命名 (如 000001.parquet)")
    print("  ✅ 指数文件: 完整格式 (如 sh.000001.parquet)")
    print("  ✅ 增量更新: 缺几天补几天，不回溯")
    print("  ✅ 增强调试: 详细日志输出")
    print()
    
    # 检查服务器数据状态
    server_latest = check_server_latest_date()
    
    # 确定更新策略
    today = datetime.now().date()
    
    if server_latest:
        days_diff = (today - server_latest).days
        
        print(f"\n📊 更新策略:")
        print(f"   服务器最新: {server_latest}")
        print(f"   今天日期: {today}")
        print(f"   相差: {days_diff} 天")
        print()
        
        if days_diff <= 0:
            print("✅ 服务器数据已是最新，无需更新")
            return
        
        # 从服务器最新日期的下一天开始
        start_date = (server_latest + timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"⏩ 增量更新: 从 {start_date} 到 {today}")
        print(f"   需要补充 {days_diff} 天数据")
        print()
        
    else:
        # 首次下载，从3个月前开始
        start_date = (today - timedelta(days=90)).strftime("%Y-%m-%d")
        print(f"📥 首次下载: 从 {start_date} 开始（最近3个月）")
        print()
    
    end_date = today.strftime("%Y-%m-%d")
    
    print(f"📅 下载区间: {start_date} ~ {end_date}")
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
        
        print(f"\n开始下载 {len(stocks)} 只股票...")
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
        print(f"   跳过: {skip_count}")
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
        print("="*50)
        print("📊 下载完成统计")
        print("="*50)
        print(f"总文件数: {total_files}")
        print(f"输出目录: {output_dir.absolute()}")
        
    except Exception as e:
        print(f"\n❌ 下载过程出错: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        bs.logout()
        print("\n✅ 已登出baostock")


if __name__ == "__main__":
    main()
