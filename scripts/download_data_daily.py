#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日增量更新阿里云轻量服务器股票数据脚本
用于GitHub Actions自动化
只下载最新的数据,不重复下载历史数据
"""

import baostock as bs
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import argparse
import sys
from tqdm import tqdm

class DailyStockUpdater:
    """每日股票数据更新器"""
    
    def __init__(self, output_dir="data/daily_parquet"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def get_date_range(self, recent_days=5, start_date=None, end_date=None):
        """
        获取要更新的日期范围
        
        Args:
            recent_days: 最近N个交易日
            start_date: 指定开始日期 (YYYY-MM-DD)
            end_date: 指定结束日期 (YYYY-MM-DD)
        """
        if start_date and end_date:
            return start_date, end_date
        
        # 默认:最近N个交易日
        end_date = datetime.now()
        start_date = end_date - timedelta(days=recent_days)
        
        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
    
    def get_all_stocks(self):
        """获取所有股票代码"""
        print("📋 获取股票列表...")
        
        # 获取沪深A股列表
        rs = bs.query_stock_basic()
        stock_list = []
        
        while rs.next():
            stock_list.append(rs.get_row_data()[0])  # 获取股票代码
        
        print(f"✅ 共找到 {len(stock_list)} 只股票")
        return stock_list
    
    def download_stock_data(self, stock_code, start_date, end_date):
        """
        下载单只股票的数据
        
        Args:
            stock_code: 股票代码 (如 sh.600000)
            start_date: 开始日期
            end_date: 结束日期
        """
        try:
            # 查询日线数据
            rs = bs.query_history_k_data_plus(
                stock_code,
                "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3"  # 不复权
            )
            
            # 收集数据
            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return None
            
            # 转换为DataFrame
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            # 数据类型转换
            numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 
                          'volume', 'amount', 'turn', 'pctChg']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            df['date'] = pd.to_datetime(df['date'])
            
            return df
            
        except Exception as e:
            print(f"❌ {stock_code} 下载失败: {e}")
            return None
    
    def update_or_append(self, stock_code, new_data):
        """
        更新或追加数据到已有的parquet文件
        
        Args:
            stock_code: 股票代码 (如 sh.600000)
            new_data: 新下载的数据
        """
        # 去掉前缀,只保留6位代码
        code = stock_code.split('.')[-1]
        file_path = self.output_dir / f"{code}.parquet"
        
        if file_path.exists():
            # 读取已有数据
            existing_data = pd.read_parquet(file_path)
            
            # 合并数据 (去重)
            combined = pd.concat([existing_data, new_data], ignore_index=True)
            combined = combined.drop_duplicates(subset=['date'], keep='last')
            combined = combined.sort_values('date').reset_index(drop=True)
            
            # 只保存有新数据的情况
            if len(combined) > len(existing_data):
                combined.to_parquet(file_path, index=False, compression='snappy')
                return len(combined) - len(existing_data)  # 返回新增记录数
            return 0
        else:
            # 新文件,直接保存
            new_data.to_parquet(file_path, index=False, compression='snappy')
            return len(new_data)
    
    def run(self, recent_days=5, start_date=None, end_date=None):
        """
        执行增量更新
        
        Args:
            recent_days: 最近N个交易日
            start_date: 指定开始日期
            end_date: 指定结束日期
        """
        print("=" * 60)
        print("📊 每日股票数据增量更新")
        print("=" * 60)
        
        # 登录baostock
        print("\n🔐 登录baostock...")
        lg = bs.login()
        if lg.error_code != '0':
            print(f"❌ 登录失败: {lg.error_msg}")
            return False
        print("✅ 登录成功")
        
        try:
            # 获取日期范围
            start_date, end_date = self.get_date_range(recent_days, start_date, end_date)
            print(f"\n📅 更新日期范围: {start_date} 到 {end_date}")
            
            # 获取股票列表
            stock_list = self.get_all_stocks()
            
            # 统计
            total_stocks = len(stock_list)
            updated_count = 0
            failed_count = 0
            new_records = 0
            
            print(f"\n🚀 开始下载数据...")
            print(f"   总股票数: {total_stocks}")
            print()
            
            # 下载数据
            with tqdm(stock_list, desc="下载进度") as pbar:
                for stock_code in pbar:
                    pbar.set_description(f"下载 {stock_code}")
                    
                    # 下载数据
                    df = self.download_stock_data(stock_code, start_date, end_date)
                    
                    if df is not None and len(df) > 0:
                        # 更新文件
                        added = self.update_or_append(stock_code, df)
                        if added > 0:
                            updated_count += 1
                            new_records += added
                    else:
                        failed_count += 1
            
            # 输出统计信息
            print("\n" + "=" * 60)
            print("📊 更新完成!")
            print("=" * 60)
            print(f"✅ 成功更新: {updated_count} 只股票")
            print(f"📝 新增记录: {new_records} 条")
            print(f"⏭️  无新数据: {total_stocks - updated_count - failed_count} 只")
            print(f"❌ 失败: {failed_count} 只")
            print(f"📂 数据目录: {self.output_dir.absolute()}")
            print("=" * 60)
            
            return True
            
        except Exception as e:
            print(f"\n❌ 更新失败: {e}")
            import traceback
            traceback.print_exc()
            return False
            
        finally:
            # 登出
            bs.logout()
            print("\n👋 已登出baostock")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='每日增量更新股票数据')
    parser.add_argument('--recent_days', type=int, default=5,
                      help='更新最近N个交易日的数据 (默认5天)')
    parser.add_argument('--start_date', type=str, default=None,
                      help='指定开始日期 YYYY-MM-DD')
    parser.add_argument('--end_date', type=str, default=None,
                      help='指定结束日期 YYYY-MM-DD')
    parser.add_argument('--output', type=str, default='data/daily_parquet',
                      help='输出目录 (默认: data/daily_parquet)')
    
    args = parser.parse_args()
    
    # 创建更新器
    updater = DailyStockUpdater(output_dir=args.output)
    
    # 执行更新
    success = updater.run(
        recent_days=args.recent_days,
        start_date=args.start_date,
        end_date=args.end_date
    )
    
    # 退出
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
