#!/usr/bin/env python3
"""
GFEX 新能源金属历史数据导入脚本
工业硅(SI) / 碳酸锂(LC) / 多晶硅(PS)

数据来源：
  - GFEX期货行情（价格/成交量/持仓量）：AkShare（get_gfex_daily）
  - GFEX仓单库存：AkShare（futures_gfex_warehouse_receipt）

设计上对齐 import_nonfer.py 的写法：
  - 批量 upsert（100条/批）+ 内存去重 + 限速
  - 每个品种写入各自独立表（不建通用大表）
  - 收尾时验证写入结果

运行：python3 import_gfex.py
      python3 import_gfex.py --date 20260618     # 抓取指定日期
      python3 import_gfex.py --backfill 30        # 回填最近30个交易日
"""

import os
import sys
import time
import math
import argparse
import datetime
from collections import Counter

import akshare as ak
import pandas as pd
from supabase import create_client

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── 配置：优先从环境变量读取，本地可用 .env 文件 ──────────
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://opgqjxkaocggconjxgpi.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

# 三个品种配置：GFEX交易代码(小写，用于AkShare查询) -> 独立表名
VARIETIES = {
    'si': {'name': '工业硅', 'table': 'industrial_silicon_records'},
    'lc': {'name': '碳酸锂', 'table': 'lithium_carbonate_records'},
    'ps': {'name': '多晶硅', 'table': 'polysilicon_records'},
}


def safe_float(v):
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


def upsert(client, table, records, label):
    """批量写入，100条/批，对齐 import_nonfer.py 的写法"""
    if not records:
        print(f"    {label}: 无数据")
        return
    for i in range(0, len(records), 100):
        batch = records[i:i + 100]
        try:
            client.table(table).upsert(
                batch, on_conflict='date,source'
            ).execute()
            print(f"    {label}: 写入 {min(i+100,len(records))}/{len(records)} 条", end='\r')
        except Exception as e:
            print(f"\n    {label} 批次失败: {e}")
        time.sleep(0.15)
    print(f"    {label}: ✓ {len(records)} 条")


def dedup(records):
    """按 (date, source) 去重，保留最后一条（与 import_nonfer.py 逻辑一致）"""
    seen, unique = set(), []
    for r in sorted(records, key=lambda x: x['date']):
        k = (r['date'], r['source'])
        if k not in seen:
            seen.add(k)
            unique.append(r)
        else:
            # 同key出现多次，用最新覆盖（先移除旧的）
            unique = [u for u in unique if (u['date'], u['source']) != k]
            unique.append(r)
            seen.add(k)
    return unique


# ════════════════════════════════════════════
# 0. 交易日历（简单工作日近似，交易所接口对非交易日自然返回空）
# ════════════════════════════════════════════
def trading_dates_recent(n: int) -> list:
    today = datetime.date.today()
    dates, d = [], today
    while len(dates) < n:
        if d.weekday() < 5:
            dates.append(d.strftime('%Y%m%d'))
        d -= datetime.timedelta(days=1)
    return list(reversed(dates))


# ════════════════════════════════════════════
# 1. GFEX 期货行情（一次拉所有品种，逐日抓取）
# ════════════════════════════════════════════
def import_gfex_quotes(client, date_list):
    """
    GFEX的日线行情接口是按"日期"查询、一次返回当天全品种数据，
    与新浪/东财那种"按品种查询、一次返回全部历史"正好相反，
    所以这里外层循环是日期，内层从结果里按品种分流。
    """
    print("\n【GFEX 期货行情（价格/成交量/持仓量）】")

    by_variety = {code: [] for code in VARIETIES}

    for date_str in date_list:
        try:
            df = ak.get_gfex_daily(date=date_str)
        except Exception as e:
            print(f"    [{date_str}] 请求失败: {e}")
            time.sleep(0.5)
            continue

        if df is None or df.empty:
            # 非交易日或当天数据未发布，正常跳过
            time.sleep(0.3)
            continue

        iso_date = f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}"

        for code in VARIETIES:
            sub = df[df['variety'].str.lower() == code]
            if sub.empty:
                continue
            # 主力合约：当日持仓量最大的合约
            sub = sub.sort_values('open_interest', ascending=False)
            main = sub.iloc[0]

            close = safe_float(main.get('close'))
            if close is None:
                continue

            by_variety[code].append({
                'date':          iso_date,
                'source':        'script',
                'gfex_price':    close,
                'gfex_settle':   safe_float(main.get('settle')),
                'gfex_vol':      safe_float(main.get('volume')),
                'gfex_oi':       safe_float(main.get('open_interest')),
                'main_contract': str(main.get('symbol')),
                'verified':      True,
                'flagged':       False,
            })

        time.sleep(0.3)

    for code, cfg in VARIETIES.items():
        name, table = cfg['name'], cfg['table']
        print(f"  [{name}] 行情...")
        records = dedup(by_variety[code])
        upsert(client, table, records, f"{name}期货行情")
        time.sleep(0.5)


# ════════════════════════════════════════════
# 2. GFEX 仓单库存（一次拉所有品种，逐日抓取）
# ════════════════════════════════════════════
def import_gfex_warehouse(client, date_list):
    print("\n【GFEX 仓单库存（交割库）】")

    by_variety = {code: [] for code in VARIETIES}

    for date_str in date_list:
        try:
            receipt_dict = ak.futures_gfex_warehouse_receipt(date=date_str)
        except Exception as e:
            print(f"    [{date_str}] 仓单请求失败: {e}")
            time.sleep(0.5)
            continue

        if not receipt_dict:
            time.sleep(0.3)
            continue

        iso_date = f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}"

        for code in VARIETIES:
            wh_df = receipt_dict.get(code.upper())
            if wh_df is None or wh_df.empty:
                continue
            total = safe_float(wh_df['今日仓单量'].sum())
            if total is None:
                continue

            by_variety[code].append({
                'date':          iso_date,
                'source':        'warehouse',
                'warehouse_inv': total,
                'verified':      True,
                'flagged':       False,
            })

        time.sleep(0.3)

    for code, cfg in VARIETIES.items():
        name, table = cfg['name'], cfg['table']
        print(f"  [{name}] 仓单...")
        records = dedup(by_variety[code])
        upsert(client, table, records, f"{name}仓单库存")
        time.sleep(0.5)


# ════════════════════════════════════════════
# 3. 验证写入结果
# ════════════════════════════════════════════
def verify(client):
    print("\n【验证写入结果】")
    for code, cfg in VARIETIES.items():
        name, table = cfg['name'], cfg['table']
        try:
            resp = client.table(table).select('source', count='exact').execute()
            counts = Counter(r['source'] for r in resp.data)
            for source, cnt in sorted(counts.items()):
                print(f"  {name:4s} {table:28s} {source:12s}: {cnt} 条")
            if not counts:
                print(f"  {name:4s} {table:28s} (空表)")
        except Exception as e:
            print(f"  {name} 验证失败: {e}")


# ════════════════════════════════════════════
# 主流程
# ════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description='GFEX 工业硅/碳酸锂/多晶硅 数据导入')
    parser.add_argument('--date', type=str, default=None,
                         help='抓取指定日期 YYYYMMDD，默认抓取今天')
    parser.add_argument('--backfill', type=int, default=None,
                         help='回填最近N个交易日（首次建表后批量补数据用）')
    args = parser.parse_args()

    print("=" * 55)
    print("  GFEX 新能源金属数据导入")
    print("  工业硅 / 碳酸锂 / 多晶硅")
    print("=" * 55)

    if not SUPABASE_KEY:
        print("⚠  请先设置 SUPABASE_KEY 环境变量")
        sys.exit(1)

    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    if args.backfill:
        date_list = trading_dates_recent(args.backfill)
        print(f"\n回填模式：候选日期 {len(date_list)} 个（{date_list[0]} ~ {date_list[-1]}）")
    else:
        date_list = [args.date or datetime.date.today().strftime('%Y%m%d')]

    import_gfex_quotes(client, date_list)
    import_gfex_warehouse(client, date_list)

    verify(client)

    print("\n✅ 全部完成")


if __name__ == '__main__':
    main()
