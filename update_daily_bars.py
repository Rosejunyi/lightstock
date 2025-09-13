name: 1. Update Daily Bars

on:
  schedule:
    - cron: '10 18 * * 1-5' # 每天北京时间凌晨 02:10 启动
  workflow_dispatch:

jobs:
  run-script:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v3

      - name: Set up Python 3.12
        uses: actions/setup-python@v4
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Run update_daily_bars.py
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
        # --- 关键修复：直接运行根目录下的脚本 ---
        run: python update_daily_bars.py
