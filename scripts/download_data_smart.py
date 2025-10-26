#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能增量更新脚本 github上更新阿里云轻量服务器的daily_parquet数据
自动检测服务器最新数据日期,只下载缺失的数据
"""

import baostock as bs
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import sys
from tqdm import tqdm

class SmartStockUpdater:
    """智能股票数据更新器"""
    
    def __init__(self, output_dir="data/daily_parquet"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
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
    
    def check_existing_data(self):
        """检查已有数据的最新日期"""
        parquet_files = list(self.output_dir.glob("*.parquet"))
        
        if not parquet_files:
            print("📭 本地无数据,将下载最近30天")
            return None
        
        # 随机检查几个文件,找最新日期
        sample_size = min(10, len(parquet_files))
        import random
        samples = random.sample(parquet_files, sample_size)
        
        latest_date = None
        for file in samples:
            try:
                df = pd.read_parquet(file)
                if len(df) > 0:
                    file_latest = pd.to_datetime(df['date']).max().date()
                    if latest_date is None or file_latest > latest_date:
                        latest_date = file_latest
            except:
                continue
        
        return latest_date
    
    def calculate_update_range(self):
        """计算需要更新的日期范围"""
        # 检查本地数据
        local_latest = self.check_existing_data()
        
        # 获取最新交易日
        expected_latest = self.get_latest_trading_date()
        
        if local_latest is None:
            # 无本地数据,下载最近30天
            start_date = expected_latest - timedelta(days=30)
            end_date = expected_latest
            days_to_update = 30
            reason = "本地无数据"
        elif local_latest >= expected_latest:
            # 数据已是最新
            print(f"✅ 数据已是最新! (最新日期: {local_latest})")
            return None, None, 0, "已是最新"
        else:
            # 计算需要更新的天数
            days_diff = (expected_latest - local_latest).days
            
            # 向前多取3天确保完整
            start_date = local_latest - timedelta(days=3)
            end_date = expected_latest
            days_to_update = days_diff + 3
            reason = f"本地最新: {local_latest}, 需更新到: {expected_latest}"
        
        return start_date, end_date, days_to_update, reason
    
    def get_all_stocks(self):
        """获取所有股票代码"""
        print("📋 获取股票列表...")
        rs = bs.query_stock_basic()
        stock_list = []
        while rs.next():
            stock_list.append(rs.get_row_data()[0])
        print(f"✅ 共 {len(stock_list)} 只股票")
        return stock_list
    
    def download_stock_data(self, stock_code, start_date, end_date):
        """下载单只股票数据"""
        try:
            rs = bs.query_history_k_data_plus(
                stock_code,
                "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg",
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                frequency="d",
                adjustflag="3"
            )
            
            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return None
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            # 数据类型转换
            numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 
                          'volume', 'amount', 'turn', 'pctChg']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df['date'] = pd.to_datetime(df['date'])
            
            return df
        except:
            return None
    
    def update_or_append(self, stock_code, new_data):
        """更新或追加数据"""
        code = stock_code.split('.')[-1]
        file_path = self.output_dir / f"{code}.parquet"
        
        if file_path.exists():
            existing = pd.read_parquet(file_path)
            combined = pd.concat([existing, new_data], ignore_index=True)
            combined = combined.drop_duplicates(subset=['date'], keep='last')
            combined = combined.sort_values('date').reset_index(drop=True)
            
            if len(combined) > len(existing):
                combined.to_parquet(file_path, index=False, compression='snappy')
                return len(combined) - len(existing)
            return 0
        else:
            new_data.to_parquet(file_path, index=False, compression='snappy')
            return len(new_data)
    
    def run(self):
        """执行智能更新"""
        print("=" * 60)
        print("🧠 智能股票数据更新")
        print("=" * 60)
        print()
        
        # 登录
        print("🔐 登录baostock...")
        lg = bs.login()
        if lg.error_code != '0':
            print(f"❌ 登录失败: {lg.error_msg}")
            return False
        print("✅ 登录成功")
        print()
        
        try:
            # 计算更新范围
            print("🔍 分析数据状态...")
            start_date, end_date, days, reason = self.calculate_update_range()
            
            print(f"📊 分析结果: {reason}")
            print()
            
            if start_date is None:
                return True  # 已是最新
            
            print("📅 更新计划:")
            print(f"   开始日期: {start_date}")
            print(f"   结束日期: {end_date}")
            print(f"   预计天数: {days} 天")
            print()
            
            # 获取股票列表
            stock_list = self.get_all_stocks()
            print()
            
            # 下载数据
            print("🚀 开始下载...")
            updated = 0
            failed = 0
            new_records = 0
            
            with tqdm(stock_list, desc="下载进度") as pbar:
                for stock_code in pbar:
                    pbar.set_description(f"处理 {stock_code}")
                    
                    df = self.download_stock_data(stock_code, start_date, end_date)
                    
                    if df is not None and len(df) > 0:
                        added = self.update_or_append(stock_code, df)
                        if added > 0:
                            updated += 1
                            new_records += added
                    else:
                        failed += 1
            
            # 统计
            print()
            print("=" * 60)
            print("✅ 更新完成!")
            print("=" * 60)
            print(f"更新股票数: {updated}")
            print(f"新增记录数: {new_records}")
            print(f"无新数据: {len(stock_list) - updated - failed}")
            print(f"失败: {failed}")
            print(f"输出目录: {self.output_dir.absolute()}")
            print("=" * 60)
            
            return True
            
        except Exception as e:
            print(f"❌ 更新失败: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            bs.logout()


def main():
    updater = SmartStockUpdater()
    success = updater.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
