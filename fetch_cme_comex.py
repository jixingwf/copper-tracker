#!/usr/bin/env python3
"""
CME COMEX 铜库存每日抓取脚本
每天从 CME 固定 URL 下载当日铜库存报告并写入 Supabase

运行方式：
  python3 fetch_cme_comex.py

环境变量（本地用 .env 文件，GitHub Actions 用 Secrets）：
  SUPABASE_URL=https://xxx.supabase.co
  SUPABASE_KEY=your_service_role_key
"""

import os, re, math, requests
from io import BytesIO
from datetime import datetime
import pandas as pd

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')

def fetch_and_write():
    print(f"[CME COMEX] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("⚠ 未配置 SUPABASE_URL / SUPABASE_KEY，跳过写入")
        return

    # ── 下载 XLS ──────────────────────────────
    url = 'https://www.cmegroup.com/delivery_reports/Copper_Stocks.xls'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        print(f"  下载成功，大小: {len(resp.content)} bytes")
    except Exception as e:
        print(f"  ✗ 下载失败: {e}")
        return

    # ── 解析 XLS ──────────────────────────────
    try:
        df = pd.read_excel(BytesIO(resp.content), header=None)
    except Exception as e:
        print(f"  ✗ 解析失败: {e}")
        return

    result = {}
    for i, row in df.iterrows():
        vals = [str(v).strip() for v in row.values if str(v).strip() != 'nan']
        if not vals:
            continue

        # 报告日期（行07：COPPER - HIGH GRADE, Report Date: MM/DD/YYYY）
        if len(vals) >= 2 and 'COPPER' in vals[0].upper() and 'Report Date' in vals[1]:
            m = re.search(r'(\d+/\d+/\d+)', vals[1])
            if m:
                result['report_date'] = datetime.strptime(
                    m.group(1), '%m/%d/%Y').strftime('%Y-%m-%d')

        # 活动日期（行08：Short Tons, Activity Date: MM/DD/YYYY）
        if len(vals) >= 2 and 'Short Tons' in vals[0] and 'Activity Date' in vals[1]:
            m = re.search(r'(\d+/\d+/\d+)', vals[1])
            if m:
                result['activity_date'] = datetime.strptime(
                    m.group(1), '%m/%d/%Y').strftime('%Y-%m-%d')

        # 总铜库存（行54：TOTAL COPPER）
        if vals[0].upper() == 'TOTAL COPPER' and len(vals) >= 7:
            try:
                prev_st  = int(vals[1].replace(',', ''))
                today_st = int(vals[6].replace(',', ''))
                result['comex_inv']      = round(today_st * 0.907185, 0)
                result['comex_inv_prev'] = round(prev_st  * 0.907185, 0)
                result['comex_net_chg']  = result['comex_inv'] - result['comex_inv_prev']
            except:
                pass

        # Registered 仓单（warranted）
        if 'TOTAL REGISTERED' in vals[0].upper() and len(vals) >= 7:
            try:
                result['comex_registered'] = round(
                    int(vals[6].replace(',', '')) * 0.907185, 0)
            except:
                pass

        # Eligible 非仓单
        if 'TOTAL ELIGIBLE' in vals[0].upper() and len(vals) >= 7:
            try:
                result['comex_eligible'] = round(
                    int(vals[6].replace(',', '')) * 0.907185, 0)
            except:
                pass

    if 'comex_inv' not in result:
        print("  ✗ 未找到 TOTAL COPPER 行，可能报告格式变化")
        return

    date = result.get('activity_date') or result.get('report_date')
    if not date:
        print("  ✗ 未找到日期")
        return

    print(f"  活动日期:    {date}")
    print(f"  总库存:      {result['comex_inv']:,.0f} 公吨")
    print(f"  Registered:  {result.get('comex_registered', 0):,.0f} 公吨")
    print(f"  Eligible:    {result.get('comex_eligible', 0):,.0f} 公吨")
    print(f"  较前日变化:  {result.get('comex_net_chg', 0):+,.0f} 公吨")

    # ── 写入 Supabase ──────────────────────────
    try:
        from supabase import create_client
        client = create_client(SUPABASE_URL, SUPABASE_KEY)

        record = {
            'date':     date,
            'comex_inv': result['comex_inv'],
            'recorder': 'cme_bot',
            'source':   'comex_stock',
            'verified': True,
            'flagged':  False,
        }

        client.table('copper_records').upsert(
            record, on_conflict='date,source'
        ).execute()
        print(f"  ✅ 写入成功")

    except ImportError:
        print("  ⚠ 请安装 supabase: pip install supabase")
    except Exception as e:
        print(f"  ✗ 写入失败: {e}")


if __name__ == '__main__':
    fetch_and_write()
