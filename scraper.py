#!/usr/bin/env python3
"""
铜基本面数据抓取脚本
========================================
抓取来源：
  - COMEX铜期货价格     → yfinance（免费）
  - SHFE 铜价 + 库存    → akshare（免费）★新增
  - LME 库存            → akshare（免费）★新增
  - CFTC 持仓报告       → cftc.gov 官方CSV（免费）
  - SMM 免费公开数据    → smm.cn

写入目标：Supabase copper_records 表
"""

import os, sys, time, json, argparse, requests, math
import pandas as pd
from datetime import datetime, date

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
}

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

def safe_float(v):
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except:
        return None

# ═══════════════════════════════════════════════════════════
# 数据抓取模块
# ═══════════════════════════════════════════════════════════

def fetch_copper_price():
    """COMEX铜期货价格 via yfinance"""
    result = {}
    try:
        import yfinance as yf
        ticker = yf.Ticker("HG=F")
        hist = ticker.history(period="2d")
        if hist.empty:
            return result
        latest = hist.iloc[-1]
        close = float(latest["Close"])
        # FIX: yfinance HG=F 现在直接返回"美元/磅"（不再是美分/磅）。
        # 美元/吨 = 美元/磅 × 2204.62 磅/吨。
        # 做个自适应：如果数值明显是"美分"量级（>50，对应铜价>$0.5/磅*2204=1100，
        # 但铜历史最低也不会到$0.005/磅），则按旧逻辑/100；否则直接使用。
        if close > 50:
            price_usd_per_ton = close / 100 * 2204.62
        else:
            price_usd_per_ton = close * 2204.62
        result['lme'] = round(price_usd_per_ton, 0)
        # FIX: 'comex_price_raw' 字段在 copper_records 表中不存在，写入会导致
        # 整条记录 upsert 失败（PGRST204），改名为 comex_raw 仅用于打印，不写入DB
        comex_raw = round(close, 4)
        result['data_date'] = hist.index[-1].strftime('%Y-%m-%d')
        print(f"  ✓ COMEX铜: ${comex_raw}/磅 → ${price_usd_per_ton:,.0f}/吨 （数据日期 {result['data_date']}）")
    except ImportError:
        print("  ⚠ 请安装 yfinance: pip install yfinance")
    except Exception as e:
        print(f"  ✗ 价格抓取失败: {e}")
    return result


def fetch_shfe_price():
    """★新增：SHFE 铜主力价格 via akshare"""
    result = {}
    try:
        import akshare as ak
        df = ak.futures_main_sina(symbol='CU0')
        if df is None or df.empty:
            print("  ✗ SHFE价格：空数据")
            return result
        latest = df.iloc[-1]
        price = safe_float(latest.get('收盘价'))
        if price:
            result['shfe'] = price
            # FIX: 记录SHFE主力合约这一行对应的真实交易日
            shfe_date = latest.get('日期')
            if shfe_date is not None:
                result['shfe_data_date'] = str(shfe_date)[:10]
            print(f"  ✓ SHFE铜主力: {price:,.0f} 元/吨 （数据日期 {result.get('shfe_data_date','—')}）")
        else:
            print("  ✗ SHFE价格：无法解析收盘价")
    except Exception as e:
        print(f"  ✗ SHFE价格抓取失败: {e}")
    return result


def fetch_shfe_inventory():
    """★新增：SHFE 铜库存 via akshare，写入独立 source=shfe_inv_99"""
    records = []
    try:
        import akshare as ak
        df = ak.futures_inventory_em(symbol='沪铜')
        if df is None or df.empty:
            print("  ✗ SHFE库存：空数据")
            return records
        for _, row in df.iterrows():
            d = str(row['日期'])[:10]
            inv = safe_float(row.get('库存'))
            if inv:
                records.append({
                    'date':     d,
                    'shfe_inv': inv,
                    'recorder': 'akshare_bot',
                    'source':   'shfe_inv_99',
                    'verified': True,
                    'flagged':  False,
                })
        if records:
            print(f"  ✓ SHFE铜库存: {len(records)} 条，最新 {records[-1]['date']} = {records[-1]['shfe_inv']:,.0f} 吨")
    except Exception as e:
        print(f"  ✗ SHFE库存抓取失败: {e}")
    return records


def fetch_lme_inventory():
    """★新增：LME 铜库存 via akshare，写入独立 source=lme_stock"""
    records = []
    try:
        import akshare as ak
        df = ak.macro_euro_lme_stock()
        if df is None or df.empty:
            print("  ✗ LME库存：空数据")
            return records
        for _, row in df.iterrows():
            d = str(row['日期'])[:10]
            inv    = safe_float(row.get('铜-库存'))
            cancel = safe_float(row.get('铜-注销仓单'))
            cancel_pct = None
            if inv and inv > 0 and cancel is not None:
                cancel_pct = round(cancel / inv * 100, 2)
            if inv:
                records.append({
                    'date':      d,
                    'lme_inv':   inv,
                    'cancelled': cancel_pct,
                    'recorder':  'akshare_bot',
                    'source':    'lme_stock',
                    'verified':  True,
                    'flagged':   False,
                })
        if records:
            print(f"  ✓ LME铜库存: {len(records)} 条，最新 {records[-1]['date']} = {records[-1]['lme_inv']:,.0f} 吨")
    except Exception as e:
        print(f"  ✗ LME库存抓取失败: {e}")
    return records


def fetch_cftc_positions():
    """CFTC持仓报告"""
    result = {}
    try:
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
        if len(fields) > 10:
            report_date = fields[2] if len(fields) > 2 else ''
            try:
                long_pos  = int(fields[8].replace(',', ''))
                short_pos = int(fields[9].replace(',', ''))
                net_long  = long_pos - short_pos
                result['cftc']       = net_long
                result['cftc_long']  = long_pos
                result['cftc_short'] = short_pos
                result['cftc_date']  = report_date
                print(f"  ✓ CFTC净多: {net_long:+,} 张（报告日：{report_date}）")
            except (ValueError, IndexError) as e:
                print(f"  ✗ CFTC字段解析失败: {e}")
    except Exception as e:
        print(f"  ✗ CFTC抓取失败: {e}")
    return result


def fetch_smm_public():
    """SMM公开页面（免费部分）"""
    result = {}
    try:
        import re
        from bs4 import BeautifulSoup
        url = "https://www.smm.cn/copper"
        resp = requests.get(url, headers=HEADERS, timeout=12)
        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, 'html.parser')
        text = soup.get_text()
        premium_match = re.findall(r'升贴水[：:]\s*([+-]?\d+(?:\.\d+)?)', text)
        if premium_match:
            result['premium'] = float(premium_match[0])
            print(f"  ✓ SMM升贴水: {result['premium']} 元/吨")
        else:
            print("  ℹ SMM升贴水未抓取到（需手动补录）")
        tc_match = re.findall(r'TC[：:\s]+\$?\s*([+-]?\d+(?:\.\d+)?)', text)
        if tc_match:
            result['tc'] = float(tc_match[0])
            print(f"  ✓ SMM TC: ${result['tc']}/干吨")
        else:
            print("  ℹ SMM TC未抓取到（需手动补录）")
    except Exception as e:
        print(f"  ✗ SMM抓取失败: {e}")
    return result


# ═══════════════════════════════════════════════════════════
# 数据验证
# ═══════════════════════════════════════════════════════════

def validate_data(data: dict):
    errors, warnings = [], []
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
    shfe, lme, ratio = data.get('shfe'), data.get('lme'), data.get('ratio')
    if shfe and lme and ratio:
        calc_ratio = shfe / lme
        if abs(calc_ratio - ratio) > 0.3:
            warnings.append(f"沪伦比价={ratio}，但SHFE/LME={calc_ratio:.3f}，差异较大")
    return len(errors) == 0, errors, warnings


# ═══════════════════════════════════════════════════════════
# 写入 Supabase
# ═══════════════════════════════════════════════════════════

def get_client():
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def write_to_supabase(data: dict) -> bool:
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("  ✗ 未配置 Supabase，跳过写入")
        return False
    try:
        client = get_client()
        resp = client.table('copper_records').upsert(
            data, on_conflict='date,source'
        ).execute()
        if resp.data:
            print(f"  ✓ 写入成功")
            return True
        else:
            print(f"  ✗ 写入失败：{resp}")
            return False
    except Exception as e:
        print(f"  ✗ 写入Supabase失败: {e}")
        return False

def write_batch(records: list, label: str):
    """批量写入"""
    if not records:
        return
    if not SUPABASE_URL or not SUPABASE_KEY:
        print(f"  ✗ 未配置 Supabase，跳过 {label}")
        return
    try:
        client = get_client()
        # 去重
        seen, unique = set(), []
        for r in sorted(records, key=lambda x: x['date']):
            k = (r['date'], r['source'])
            if k not in seen:
                seen.add(k)
                unique.append(r)
        for i in range(0, len(unique), 100):
            batch = unique[i:i+100]
            client.table('copper_records').upsert(
                batch, on_conflict='date,source'
            ).execute()
            print(f"  {label}: 写入 {min(i+100,len(unique))}/{len(unique)} 条", end='\r')
        print(f"  ✓ {label}: 共 {len(unique)} 条")
    except Exception as e:
        print(f"  ✗ {label} 写入失败: {e}")

def check_duplicate(target_date: str) -> bool:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    try:
        client = get_client()
        resp = client.table('copper_records').select('id') \
            .eq('date', target_date).eq('source', 'script').execute()
        return len(resp.data) > 0
    except:
        return False


# ═══════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════

def run_once(dry_run: bool = False):
    today = date.today().isoformat()
    print(f"\n{'='*55}")
    print(f"  铜数据抓取脚本  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*55}\n")

    if True:
        # 主记录（价格 + CFTC）
        record = {
            'recorder': 'scraper_bot',
            'source':   'script',
            'verified': True,   # 自动写入直接标记已验证
            'flagged':  False,
        }

        print("[1/3] COMEX 铜期货价格（yfinance）")
        record.update(fetch_copper_price())
        time.sleep(1)

        print("[2/3] SHFE 铜主力价格（akshare）")
        record.update(fetch_shfe_price())
        time.sleep(1)

        print("[3/3] CFTC 持仓报告")
        record.update(fetch_cftc_positions())
        time.sleep(1)

        # FIX: 用数据本身的最新交易日作为 date，而不是脚本运行的 today
        # 取 COMEX / SHFE 两者中较新的数据日期；都没有则回退用 today
        candidate_dates = [d for d in (record.pop('data_date', None),
                                        record.pop('shfe_data_date', None)) if d]
        record['date'] = max(candidate_dates) if candidate_dates else today
        if record['date'] != today:
            print(f"  ℹ 数据交易日为 {record['date']}（与脚本运行日 {today} 不同，按数据日期写入）")

        # 自动计算沪伦比价
        if record.get('shfe') and record.get('lme'):
            record['ratio'] = round(record['shfe'] / record['lme'], 3)
            print(f"  ✓ 沪伦比价: {record['ratio']}")

        print("[+] SMM 公开数据")
        record.update(fetch_smm_public())

        print("\n── 数据验证 ──────────────────────────")
        is_valid, errors, warnings = validate_data(record)
        if errors:
            print(f"  ⚠ 验证警告: {'; '.join(errors)}")
            record['verification_notes'] = '; '.join(errors)
        if warnings:
            for w in warnings:
                print(f"  ⚠ {w}")

        if not dry_run:
            print("\n── 写入主记录 ────────────────────────")
            write_to_supabase(record)

    # ── 库存数据（每次都更新，不受重复检查限制）──
    print("\n── 库存数据更新 ──────────────────────")

    print("[库存1/2] LME 铜库存（akshare）")
    lme_records = fetch_lme_inventory()
    if not dry_run:
        write_batch(lme_records, 'LME库存')
    time.sleep(1)

    print("[库存2/2] SHFE 铜库存（akshare）")
    shfe_records = fetch_shfe_inventory()
    if not dry_run:
        write_batch(shfe_records, 'SHFE库存')

    print(f"\n✅ 完成  {datetime.now().strftime('%H:%M:%S')}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--once',     action='store_true', help='立即执行一次')
    parser.add_argument('--schedule', action='store_true', help='定时模式（每天09:30）')
    parser.add_argument('--dry-run',  action='store_true', help='不写入数据库')
    parser.add_argument('--force',    action='store_true', help='强制写入（忽略重复检查）')
    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("⚠  未找到 SUPABASE_URL 或 SUPABASE_KEY 环境变量")

    if args.once or args.force:
        run_once(dry_run=args.dry_run)
    elif args.schedule:
        try:
            import schedule
            print("定时模式启动，每天 09:30 执行...")
            schedule.every().day.at("09:30").do(run_once)
            while True:
                schedule.run_pending()
                time.sleep(60)
        except ImportError:
            print("请安装 schedule: pip install schedule")
    else:
        run_once()
