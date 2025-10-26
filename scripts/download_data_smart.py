#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能增量更新脚本 V3 - 完全匹配本地脚本逻辑
- 股票：纯数字命名（000001.parquet）
- 指数：带前缀命名（sh.000001.parquet）
- 支持SSH检测阿里云服务器数据状态
- 自动向前推3天更新策略
"""

import baostock as bs
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import sys
from tqdm import tqdm
import subprocess
import os


class SmartStockUpdater:
    """智能股票数据更新器 V3 - 匹配本地脚本逻辑"""
    
    def __init__(self, output_dir="data/daily_parquet", check_server=True):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.check_server = check_server
        
        # SSH配置（从环境变量读取）
        self.server_ip = os.environ.get('SERVER_IP', '')
        self.server_path = os.environ.get('SERVER_PATH', '/root/lightstock')
        
        # 指数列表（使用完整的baostock代码作为文件名）
        self.index_list = [
            "sh.000300",  # 沪深300
            "sh.000001",  # 上证指数
            "sz.399001",  # 深证成指
            "sz.399006",  # 创业板指
        ]
    
    def get_filename(self, baostock_code, is_index=False):
        """
        根据代码类型返回正确的文件名
        
        股票：sz.000001 -> 000001.parquet
        指数：sh.000001 -> sh.000001.parquet
        """
        if is_index:
            # 指数：保持完整的baostock格式
            return f"{baostock_code}.parquet"
        else:
            # 股票：只取数字部分
            if '.' in baostock_code:
                code = baostock_code.split('.')[-1]
            else:
                code = baostock_code
            return f"{code}.parquet"
    
    def get_latest_trading_date(self):
        """获取最新交易日"""
        today = datetime.now()
        
        # 周末调整
        if today.weekday() == 5:  # 周六
            return (today - timedelta(days=1)).date()
        elif today.weekday() == 6:  # 周日
            return (today - timedelta(days=2)).date()
        
        # 如果当前时间在收盘前(15:00),使用昨天
        if today.hour < 15:
            return (today - timedelta(days=1)).date()
        
        return today.date()
    
    def check_server_latest_date(self):
        """
        SSH到阿里云服务器检查最新数据日期
        返回最新日期，如果无法连接则返回None
        """
        if not self.check_server or not self.server_ip:
            print("⚠️  跳过服务器检查（未配置或未启用）")
            return None
        
        try:
            print(f"🔍 连接服务器检查数据: {self.server_ip}")
            
            # SSH命令：读取服务器上随机几个文件的最新日期
            ssh_cmd = f"""
            ssh -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa root@{self.server_ip} '
            cd {self.server_path}/data/daily_parquet/ 2>/dev/null || exit 1
            python3 << "PYEOF"
import pandas as pd
from pathlib import Path
import random

parquet_files = list(Path(".").glob("*.parquet"))
if not parquet_files:
    print("NO_DATA")
    exit(0)

# 随机抽样10个文件
samples = random.sample(parquet_files, min(10, len(parquet_files)))

latest = None
for f in samples:
    try:
        df = pd.read_parquet(f)
        date_col = "日期" if "日期" in df.columns else "date"
        if date_col in df.columns and len(df) > 0:
            file_latest = pd.to_datetime(df[date_col]).max()
            if latest is None or file_latest > latest:
                latest = file_latest
    except:
        continue

if latest:
    print(latest.strftime("%Y-%m-%d"))
else:
    print("NO_DATA")
PYEOF
            '
            """
            
            result = subprocess.run(
                ssh_cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                output = result.stdout.strip()
                if output == "NO_DATA":
                    print("📭 服务器无数据")
                    return None
                else:
                    server_date = datetime.strptime(output, '%Y-%m-%d').date()
                    print(f"✅ 服务器最新数据: {server_date}")
                    return server_date
            else:
                print(f"⚠️  无法检查服务器数据: {result.stderr[:200]}")
                return None
                
        except Exception as e:
            print(f"⚠️  服务器检查失败: {e}")
            return None
    
    def check_local_latest_date(self):
        """检查本地GitHub环境的最新数据日期"""
        parquet_files = list(self.output_dir.glob("*.parquet"))
        
        if not parquet_files:
            print("📭 本地无数据")
            return None
        
        # 随机检查几个文件
        import random
        sample_size = min(10, len(parquet_files))
        samples = random.sample(parquet_files, sample_size)
        
        latest_date = None
        for file in samples:
            try:
                df = pd.read_parquet(file)
                date_col = '日期' if '日期' in df.columns else 'date'
                if date_col in df.columns and len(df) > 0:
                    file_latest = pd.to_datetime(df[date_col]).max().date()
                    if latest_date is None or file_latest > latest_date:
                        latest_date = file_latest
            except:
                continue
        
        if latest_date:
            print(f"✅ 本地最新数据: {latest_date}")
        return latest_date
    
    def calculate_update_range(self):
        """
        计算需要更新的日期范围
        优先检查服务器数据，如果无法连接则检查本地数据
        """
        print("\n" + "="*60)
        print("🔍 检测数据状态")
        print("="*60)
        
        # 获取预期的最新交易日
        expected_latest = self.get_latest_trading_date()
        print(f"📅 预期最新交易日: {expected_latest}")
        
        # 优先检查服务器数据
        server_latest = self.check_server_latest_date()
        
        # 如果服务器检查失败，检查本地数据
        if server_latest is None:
            print("ℹ️  改为检查本地数据...")
            latest_date = self.check_local_latest_date()
            data_source = "本地"
        else:
            latest_date = server_latest
            data_source = "服务器"
        
        print()
        
        # 决策逻辑
        if latest_date is None:
            # 无数据，下载最近30天
            start_date = expected_latest - timedelta(days=30)
            end_date = expected_latest
            days_to_update = 30
            reason = f"无{data_source}数据，下载最近30天"
            
        elif latest_date >= expected_latest:
            # 数据已是最新
            print(f"✅ {data_source}数据已是最新!")
            print(f"   最新日期: {latest_date}")
            print(f"   预期日期: {expected_latest}")
            return None, None, 0, "已是最新"
            
        else:
            # 需要更新：向前推3天确保完整
            days_diff = (expected_latest - latest_date).days
            start_date = latest_date - timedelta(days=3)
            end_date = expected_latest
            days_to_update = days_diff + 3
            reason = f"{data_source}最新: {latest_date}, 需更新到: {expected_latest} (向前推3天)"
        
        return start_date, end_date, days_to_update, reason
    
    def get_stock_list(self):
        """获取A股股票列表"""
        print("📋 获取A股股票列表...")
        
        # 尝试从本地文件读取
        list_file = Path("data/market_lists/stock_list_A_share.csv")
        if list_file.exists():
            try:
                df = pd.read_csv(list_file, dtype={'code': str})
                stock_codes = df['code'].tolist()
                print(f"✅ 从本地文件读取 {len(stock_codes)} 只A股")
                return stock_codes
            except:
                pass
        
        # 从baostock获取
        rs = bs.query_stock_basic()
        stock_codes = []
        while rs.next():
            code = rs.get_row_data()[0]
            # 只要A股（6开头的沪市，0/3开头的深市）
            if code.startswith('sh.6') or code.startswith('sz.0') or code.startswith('sz.3'):
                stock_codes.append(code)
        
        print(f"✅ 从baostock获取 {len(stock_codes)} 只A股")
        return stock_codes
    
    def download_data(self, code, start_date, end_date, is_index=False):
        """下载单个代码的数据"""
        try:
            if is_index:
                # 指数数据
                fields = "date,code,open,high,low,close,volume,amount"
                rs = bs.query_history_k_data_plus(
                    code,
                    fields,
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d'),
                    frequency="d"
                )
            else:
                # 股票数据
                fields = "date,code,open,high,low,close,volume,amount,adjustflag"
                rs = bs.query_history_k_data_plus(
                    code,
                    fields,
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d'),
                    frequency="d",
                    adjustflag="3"  # 不复权
                )
            
            if rs.error_code != '0':
                return None
            
            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return None
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            # 数据清洗和重命名
            rename_dict = {
                'date': '日期',
                'open': '开盘',
                'high': '最高',
                'low': '最低',
                'close': '收盘',
                'volume': '成交量',
                'amount': '成交额'
            }
            df.rename(columns=rename_dict, inplace=True)
            
            # 转换数值类型
            numeric_cols = ['开盘', '最高', '最低', '收盘', '成交量', '成交额']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df['日期'] = pd.to_datetime(df['日期'])
            
            return df
        except:
            return None
    
    def update_or_append(self, code, new_data, is_index=False):
        """
        更新或追加数据
        使用正确的命名规范：
        - 股票：纯数字（000001.parquet）
        - 指数：带前缀（sh.000001.parquet）
        """
        filename = self.get_filename(code, is_index)
        file_path = self.output_dir / filename
        
        if file_path.exists():
            # 文件存在，合并数据
            existing = pd.read_parquet(file_path)
            
            combined = pd.concat([existing, new_data], ignore_index=True)
            combined = combined.drop_duplicates(subset=['日期'], keep='last')
            combined = combined.sort_values('日期').reset_index(drop=True)
            
            if len(combined) > len(existing):
                combined.to_parquet(file_path, index=False, compression='snappy')
                return len(combined) - len(existing)
            return 0
        else:
            # 新文件
            new_data.to_parquet(file_path, index=False, compression='snappy')
            return len(new_data)
    
    def run(self):
        """执行智能更新"""
        print("\n" + "="*60)
        print("🧠 智能股票数据更新 V3")
        print("="*60)
        print()
        
        # 登录baostock
        print("🔐 登录baostock...")
        lg = bs.login()
        if lg.error_code != '0':
            print(f"❌ 登录失败: {lg.error_msg}")
            return False
        print("✅ 登录成功")
        
        try:
            # 计算更新范围
            start_date, end_date, days, reason = self.calculate_update_range()
            
            print("\n" + "="*60)
            print("📊 更新策略")
            print("="*60)
            print(f"策略: {reason}")
            
            if start_date is None:
                print("\n✅ 无需更新，数据已是最新!")
                return True
            
            print(f"开始日期: {start_date}")
            print(f"结束日期: {end_date}")
            print(f"预计天数: {days} 天")
            print("="*60)
            print()
            
            # 统计变量
            updated_count = 0
            failed_count = 0
            new_records = 0
            
            # 1. 更新指数数据
            print("📈 更新指数数据...")
            print(f"ℹ️  指数文件命名: 保持完整格式（如 sh.000001.parquet）\n")
            
            with tqdm(self.index_list, desc="指数进度") as pbar:
                for index_code in pbar:
                    pbar.set_description(f"处理 {index_code}")
                    
                    df = self.download_data(index_code, start_date, end_date, is_index=True)
                    
                    if df is not None and len(df) > 0:
                        added = self.update_or_append(index_code, df, is_index=True)
                        if added > 0:
                            updated_count += 1
                            new_records += added
                    else:
                        failed_count += 1
            
            print()
            
            # 2. 更新股票数据
            print("📊 更新A股数据...")
            print(f"ℹ️  股票文件命名: 纯数字代码（如 000001.parquet）\n")
            
            stock_list = self.get_stock_list()
            
            with tqdm(stock_list, desc="股票进度") as pbar:
                for stock_code in pbar:
                    # 显示格式：sh.600000 -> 600000.parquet
                    display_name = self.get_filename(stock_code, is_index=False)
                    pbar.set_description(f"处理 {stock_code} -> {display_name}")
                    
                    df = self.download_data(stock_code, start_date, end_date, is_index=False)
                    
                    if df is not None and len(df) > 0:
                        added = self.update_or_append(stock_code, df, is_index=False)
                        if added > 0:
                            updated_count += 1
                            new_records += added
                    else:
                        failed_count += 1
            
            # 统计
            print()
            print("="*60)
            print("✅ 更新完成!")
            print("="*60)
            print(f"✅ 更新数量: {updated_count}")
            print(f"✅ 新增记录: {new_records}")
            print(f"ℹ️  无新数据: {len(stock_list) + len(self.index_list) - updated_count - failed_count}")
            print(f"❌ 失败: {failed_count}")
            print(f"📁 输出目录: {self.output_dir.absolute()}")
            print()
            print("📝 文件命名规范:")
            print(f"   股票: 纯数字（如 000001.parquet）")
            print(f"   指数: 完整格式（如 sh.000001.parquet）")
            print("="*60)
            
            return True
            
        except Exception as e:
            print(f"❌ 更新失败: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            bs.logout()


def main():
    """
    主函数
    支持环境变量:
    - SERVER_IP: 阿里云服务器IP
    - SERVER_PATH: 服务器上的项目路径（默认: /root/lightstock）
    - CHECK_SERVER: 是否检查服务器数据（默认: true）
    """
    check_server = os.environ.get('CHECK_SERVER', 'true').lower() == 'true'
    
    updater = SmartStockUpdater(check_server=check_server)
    success = updater.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
