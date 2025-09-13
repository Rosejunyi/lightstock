name: 3. Calculate Indicators

on:
  workflow_run:
    workflows: ["2. Update Daily Metrics"] # 在“获取指标”成功后触发
    types:
      - completed
  workflow_dispatch:

jobs:
  run-script:
    runs-on: ubuntu-latest
    if: ${{ github.event.workflow_run.conclusion == 'success' }}
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

      - name: Run calculate_indicators.py
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
        # --- 关键修复：直接运行根目录下的脚本 ---
        run: python calculate_indicators.py
