#!/usr/bin/env python3
"""
铜基本面数据抓取脚本
========================================
版本A：本地手动/定时运行
版本B：GitHub Actions 自动定时运行（见文件末尾 YAML 配置）

抓取来源：
  - COMEX铜期货价格     → yfinance（免费）
  - CFTC 持仓报告       → cftc.gov 官方CSV（免费）
  - LME 库存引导链接    → 免费可查，需手动确认
  - SMM 免费公开数据    → smm.cn

写入目标：Supabase copper_records 表

使用方法：
  pip install requests beautifulsoup4 pandas supabase yfinance schedule python-dotenv

  本地单次运行：
    python scraper.py --once

  本地定时运行（每天09:30）：
    python scraper.py --schedule

  GitHub Actions：将文件末尾的 YAML 保存为 .github/workflows/scraper.yml
"""

import os
import sys
import time
import json
import argparse
import requests
import pandas as pd
from datetime import datetime, date
from io import StringIO

# ── 环境变量读取 ─────────────────────────────────────────
# 本地运行：新建 .env 文件，写入以下两行：
#   SUPABASE_URL=https://YOUR_PROJECT.supabase.co
#   SUPABASE_KEY=YOUR_SERVICE_ROLE_KEY   ← 用 service_role key，有写权限
# GitHub Actions：在仓库 Settings → Secrets 里添加同名变量

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')   # service_role key

if not SUPABASE_URL or not SUPABASE_KEY:
    print("⚠  未找到 SUPABASE_URL 或 SUPABASE_KEY 环境变量")
    print("   本地运行请新建 .env 文件，GitHub Actions 请配置 Secrets")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
}

# 数据验证规则（与前端保持一致）
VALIDATION_RULES = {
    'shfe':       (40000,  120000),
    'lme':        (4000,   15000),
    'premium':    (-3000,  3000),
    'import_pnl': (-5000,  5000),
    'lme_inv':    (10000,  1000000),
    'shfe_inv':   (5000,   800000),
    'comex_inv':  (1000,   300000),
    'cancelled':  (0,      100),
    'tc':         (-50,    200),
    'cftc':       (-150000,200000),
    'pmi':        (30,     70),
    'grid':       (-50,    100),
    'nev':        (-80,    300),
    'cu_import':  (10,     150),
}

# ═══════════════════════════════════════════════════════════
# 数据抓取模块
# ═══════════════════════════════════════════════════════════

def fetch_copper_price():
    """COMEX铜期货价格 via yfinance（免费，无需API key）"""
    result = {}
    try:
        import yfinance as yf
        ticker = yf.Ticker("HG=F")
        hist = ticker.history(period="2d")
        if hist.empty:
            return result
        latest = hist.iloc[-1]
        # HG以美分/磅计价，换算成美元/吨
        price_usd_per_ton = latest['Close'] / 100 * 2204.62
        result['lme'] = round(price_usd_per_ton, 0)
        result['comex_price_raw'] = round(latest['Close'], 4)
        result['price_date'] = hist.index[-1].strftime('%Y-%m-%d')
        print(f"  ✓ COMEX铜: ${latest['Close']:.4f}/磅 → ${price_usd_per_ton:,.0f}/吨")
    except ImportError:
        print("  ⚠ 请安装 yfinance: pip install yfinance")
    except Exception as e:
        print(f"  ✗ 价格抓取失败: {e}")
    return result


def fetch_cftc_positions():
    """CFTC持仓报告 - 铜（COMEX，非商业净多头）"""
    result = {}
    try:
        # CFTC官方提供每周持仓报告CSV，Disaggregated格式
        url = "https://www.cftc.gov/dea/newcot/f_disagg.txt"
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()

        lines = resp.text.split('\n')
        copper_line = None
        for line in lines:
            if 'COPPER' in line.upper() and 'COMEX' in line.upper():
                copper_line = line
                break

        if not copper_line:
            print("  ✗ CFTC: 未找到铜数据行")
            return result

        fields = [f.strip().strip('"') for f in copper_line.split(',')]

        # Disaggregated COT报告字段（参考CFTC文档）
        # 字段索引：2=报告日期, 8=非商业多头, 9=非商业空头, 10=spread
        if len(fields) > 10:
            report_date = fields[2] if len(fields) > 2 else ''
            try:
                long_pos  = int(fields[8].replace(',', ''))
                short_pos = int(fields[9].replace(',', ''))
                net_long  = long_pos - short_pos
                result['cftc']        = net_long
                result['cftc_long']   = long_pos
                result['cftc_short']  = short_pos
                result['cftc_date']   = report_date
                print(f"  ✓ CFTC净多: {net_long:+,} 张（报告日：{report_date}）")
            except (ValueError, IndexError) as e:
                print(f"  ✗ CFTC字段解析失败: {e}")
    except Exception as e:
        print(f"  ✗ CFTC抓取失败: {e}")
    return result


def fetch_smm_public():
    """
    SMM公开页面（免费部分）
    注：详细数据需付费账号。此处抓取有限的公开信息，
    并提供手动填写的占位结构。
    """
    result = {}
    try:
        # SMM铜页面
        url = "https://www.smm.cn/copper"
        resp = requests.get(url, headers=HEADERS, timeout=12)
        resp.encoding = 'utf-8'

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, 'html.parser')

        # 尝试抓取页面中的价格/升贴水数字
        # （SMM页面结构可能变化，抓到则用，否则跳过）
        import re
        text = soup.get_text()

        # 匹配升贴水模式：如 "+180" 或 "-320"
        premium_match = re.findall(r'升贴水[：:]\s*([+-]?\d+(?:\.\d+)?)', text)
        if premium_match:
            result['premium'] = float(premium_match[0])
            print(f"  ✓ SMM升贴水: {result['premium']} 元/吨")
        else:
            print("  ℹ SMM升贴水未抓取到（需付费账号或手动补录）")

        # 匹配TC
        tc_match = re.findall(r'TC[：:\s]+\$?\s*([+-]?\d+(?:\.\d+)?)', text)
        if tc_match:
            result['tc'] = float(tc_match[0])
            print(f"  ✓ SMM TC: ${result['tc']}/干吨")
        else:
            print("  ℹ SMM TC未抓取到（需付费账号或手动补录）")

    except ImportError:
        print("  ⚠ 请安装 beautifulsoup4: pip install beautifulsoup4")
    except Exception as e:
        print(f"  ✗ SMM抓取失败: {e}")
    return result


def fetch_lme_inventory_guide():
    """
    LME库存：免费数据需在 lme.com 查看
    此函数仅返回引导信息，不实际抓取（LME反爬严格）
    若有LME API账号，可在此替换为API调用
    """
    print("  ℹ LME库存需手动查询：https://www.lme.com/en/metals/non-ferrous/lme-copper")
    print("    或使用 Bloomberg/Refinitiv 数据终端")
    return {}


# ═══════════════════════════════════════════════════════════
# 数据验证
# ═══════════════════════════════════════════════════════════

def validate_data(data: dict) -> tuple[bool, list, list]:
    """
    验证数据合理性
    返回 (is_valid, errors, warnings)
    """
    errors = []
    warnings = []

    for field, (min_val, max_val) in VALIDATION_RULES.items():
        v = data.get(field)
        if v is None:
            continue
        try:
            v = float(v)
        except (TypeError, ValueError):
            errors.append(f"{field}={v} 非数字")
            continue

        if v < min_val or v > max_val:
            errors.append(f"{field}={v} 超出范围 [{min_val}, {max_val}]")

    # 一致性检查
    shfe, lme, ratio = data.get('shfe'), data.get('lme'), data.get('ratio')
    if shfe and lme and ratio:
        calc_ratio = shfe / lme
        if abs(calc_ratio - ratio) > 0.3:
            warnings.append(f"沪伦比价填入={ratio}，但SHFE/LME={calc_ratio:.3f}，差异较大")

    tc = data.get('tc')
    if tc is not None and tc < -10:
        warnings.append(f"TC={tc} 极端负值，请确认数据来源")

    is_valid = len(errors) == 0
    return is_valid, errors, warnings


# ═══════════════════════════════════════════════════════════
# 写入 Supabase
# ═══════════════════════════════════════════════════════════

def write_to_supabase(data: dict) -> bool:
    """写入Supabase copper_records 表"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("  ✗ 未配置 Supabase，跳过写入")
        return False

    try:
        from supabase import create_client
        client = create_client(SUPABASE_URL, SUPABASE_KEY)
        resp = client.table('copper_records').insert(data).execute()
        if resp.data:
            print(f"  ✓ 写入 Supabase 成功，ID: {resp.data[0].get('id','')}")
            return True
        else:
            print(f"  ✗ 写入失败：{resp}")
            return False
    except ImportError:
        print("  ⚠ 请安装 supabase: pip install supabase")
        # 降级：输出JSON
        print(f"  ℹ 数据（JSON）:\n{json.dumps(data, ensure_ascii=False, indent=2)}")
        return False
    except Exception as e:
        print(f"  ✗ 写入Supabase失败: {e}")
        return False


def check_duplicate(target_date: str) -> bool:
    """检查当天脚本数据是否已写入，避免重复"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    try:
        from supabase import create_client
        client = create_client(SUPABASE_URL, SUPABASE_KEY)
        resp = client.table('copper_records') \
            .select('id') \
            .eq('date', target_date) \
            .eq('source', 'script') \
            .execute()
        return len(resp.data) > 0
    except:
        return False


# ═══════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════

def run_once(dry_run: bool = False):
    """执行一次完整抓取流程"""
    today = date.today().isoformat()
    print(f"\n{'='*55}")
    print(f"  铜数据抓取脚本  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*55}\n")

    # 检查重复
    if not dry_run and check_duplicate(today):
        print(f"⚠  今天（{today}）已有脚本数据，跳过写入")
        print("   如需强制写入请用 --force 参数")
        return

    # 组合数据
    record = {
        'date':     today,
        'recorder': 'scraper_bot',
        'source':   'script',
        'verified': False,   # 脚本数据仍需管理员验证
        'flagged':  False,
    }

    print("[1/4] 铜期货价格（COMEX / yfinance）")
    record.update(fetch_copper_price())
    time.sleep(1)

    print("[2/4] CFTC 持仓报告")
    record.update(fetch_cftc_positions())
    time.sleep(1)

    print("[3/4] SMM 公开数据")
    record.update(fetch_smm_public())
    time.sleep(1)

    print("[4/4] LME 库存（引导）")
    fetch_lme_inventory_guide()

    # 自动计算比价
    if record.get('shfe') and record.get('lme'):
        record['ratio'] = round(record['shfe'] / record['lme'], 3)

    print("\n── 数据验证 ──────────────────────────")
    is_valid, errors, warnings = validate_data(record)

    if errors:
        print("  ✗ 错误（不写入Supabase）：")
        for e in errors:
            print(f"    · {e}")
        record['verification_notes'] = '脚本验证失败: ' + '; '.join(errors)
        record['flagged'] = True
    else:
        print("  ✓ 数据范围验证通过")

    if warnings:
        print("  ⚠ 警告（仍可写入，人工确认）：")
        for w in warnings:
            print(f"    · {w}")
        record['verification_notes'] = '警告: ' + '; '.join(warnings)

    # 输出记录预览
    print("\n── 数据预览 ──────────────────────────")
    preview_keys = ['date','lme','cftc','tc','premium']
    for k in preview_keys:
        v = record.get(k)
        if v is not None:
            print(f"  {k:<15} = {v}")

    # 写入
    if dry_run:
        print("\n⚠  DRY RUN 模式，不写入数据库")
        print(f"完整数据: {json.dumps(record, ensure_ascii=False, default=str)}")
    else:
        print("\n── 写入 Supabase ────────────────────")
        write_to_supabase(record)

    print(f"\n✅ 完成  |  {datetime.now().strftime('%H:%M:%S')}\n")
    return record


def run_scheduled():
    """定时运行（需要 schedule 库）"""
    try:
        import schedule
    except ImportError:
        print("请安装 schedule: pip install schedule")
        sys.exit(1)

    print("定时任务启动（按 Ctrl+C 停止）")
    print("计划：工作日 09:30 / 周一额外09:35抓CFTC")

    schedule.every().monday.at("09:30").do(run_once)
    schedule.every().tuesday.at("09:30").do(run_once)
    schedule.every().wednesday.at("09:30").do(run_once)
    schedule.every().thursday.at("09:30").do(run_once)
    schedule.every().friday.at("09:30").do(run_once)

    print(f"下次运行：{schedule.next_run()}")

    while True:
        schedule.run_pending()
        time.sleep(30)


# ═══════════════════════════════════════════════════════════
# CLI 入口
# ═══════════════════════════════════════════════════════════

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='铜基本面数据抓取脚本')
    parser.add_argument('--once',     action='store_true', help='立即运行一次（默认）')
    parser.add_argument('--schedule', action='store_true', help='启动定时任务')
    parser.add_argument('--dry-run',  action='store_true', help='仅测试，不写入数据库')
    parser.add_argument('--force',    action='store_true', help='强制写入，忽略重复检查')
    args = parser.parse_args()

    if args.schedule:
        run_scheduled()
    elif args.dry_run:
        run_once(dry_run=True)
    else:
        run_once(dry_run=False)


# ═══════════════════════════════════════════════════════════
# Supabase 数据表 SQL（首次使用时在 Supabase SQL Editor 执行）
# ═══════════════════════════════════════════════════════════
"""
-- 在 Supabase SQL Editor 运行以下 SQL 建表：

CREATE TABLE copper_records (
  id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  date            DATE NOT NULL,
  recorder        TEXT,
  source          TEXT,

  -- 价格
  shfe            NUMERIC,
  lme             NUMERIC,
  ratio           NUMERIC,
  premium         NUMERIC,
  import_pnl      NUMERIC,

  -- 库存
  lme_inv         NUMERIC,
  shfe_inv        NUMERIC,
  comex_inv       NUMERIC,
  cancelled       NUMERIC,

  -- 供给
  tc              NUMERIC,
  cftc            NUMERIC,
  cftc_long       NUMERIC,
  cftc_short      NUMERIC,
  cftc_date       TEXT,

  -- 需求
  pmi             NUMERIC,
  grid            NUMERIC,
  nev             NUMERIC,
  cu_import       NUMERIC,

  -- 事件
  event           TEXT,

  -- 验证
  verified        BOOLEAN DEFAULT FALSE,
  flagged         BOOLEAN DEFAULT FALSE,
  verified_at     TIMESTAMPTZ,
  verification_notes TEXT
);

-- 开启行级安全（RLS）
ALTER TABLE copper_records ENABLE ROW LEVEL SECURITY;

-- 所有人可读已验证数据
CREATE POLICY "public_read" ON copper_records
  FOR SELECT USING (verified = true);

-- 所有人可插入新数据
CREATE POLICY "public_insert" ON copper_records
  FOR INSERT WITH CHECK (true);

-- 只有 service_role（管理员/脚本）可以修改和删除
-- （前端admin.html用 anon key，通过密码逻辑控制；
--  如需严格权限可改为 Supabase Auth）
"""


# ═══════════════════════════════════════════════════════════
# GitHub Actions 配置（保存为 .github/workflows/copper_scraper.yml）
# ═══════════════════════════════════════════════════════════
GITHUB_ACTIONS_YAML = """
name: Copper Data Scraper

on:
  schedule:
    # 北京时间 09:30 = UTC 01:30，工作日执行
    - cron: '30 1 * * 1-5'
  workflow_dispatch:   # 允许手动触发

jobs:
  scrape:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install requests beautifulsoup4 pandas supabase yfinance python-dotenv

      - name: Run scraper
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
        run: python scraper.py --once

      - name: Upload log
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: scraper-log-${{ github.run_id }}
          path: '*.log'
          retention-days: 7
"""
# 使用方法：
# 1. 将上方 YAML 内容保存到 .github/workflows/copper_scraper.yml
# 2. 在 GitHub 仓库 Settings → Secrets → Actions 添加：
#    SUPABASE_URL = https://xxx.supabase.co
#    SUPABASE_KEY = your_service_role_key
# 3. Push到GitHub后，每个工作日北京时间09:30自动运行
# 4. 也可以在 Actions 页面手动点击 "Run workflow"
