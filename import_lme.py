#!/usr/bin/env python3
"""
AkShare 拉取 LME 铜库存+持仓，写入 Supabase
用法: python3 import_lme.py
"""
import akshare as ak
import pandas as pd
import time
import json
from supabase import create_client

SUPABASE_URL = 'https://opgqjxkaocggconjxgpi.supabase.co'
SUPABASE_KEY = 'sb_secret_wMEiaD2wNjcwYeOD7fPagQ_SlEI8jqj'

client = create_client(SUPABASE_URL, SUPABASE_KEY)

def safe_float(v):
    """把任何值安全转为 float 或 None，NaN/Inf 都返回 None"""
    try:
        import math
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except:
        return None

def clean_record(d):
    """把 dict 里所有的 NaN/float('nan') 替换为 None"""
    out = {}
    for k, v in d.items():
        out[k] = safe_float(v) if isinstance(v, float) else v
    return out

def upsert_batch(records, label):
    """分批写入，每批50条"""
    cleaned = [clean_record(r) for r in records]
    success = 0
    for i in range(0, len(cleaned), 50):
        batch = cleaned[i:i+50]
        try:
            client.table('copper_records').upsert(
                batch, on_conflict='date,source'
            ).execute()
            success += len(batch)
            print(f'  {label}: 写入 {min(i+50, len(cleaned))}/{len(cleaned)} 条', end='\r')
        except Exception as e:
            print(f'\n  批次失败: {e}')
        time.sleep(0.2)
    print()
    return success

# ══════════════════════════════════════
# 1. LME 库存（铜-库存、注销仓单率）
# ══════════════════════════════════════
print('[ 1/2 ] 拉取 LME 铜库存...')
df = ak.macro_euro_lme_stock()

records = []
for _, row in df.iterrows():
    date = str(row['日期'])[:10]
    inv     = safe_float(row.get('铜-库存'))
    cancel  = safe_float(row.get('铜-注销仓单'))
    cancel_pct = None
    if inv and inv > 0 and cancel is not None:
        cancel_pct = round(cancel / inv * 100, 2)

    records.append({
        'date':      date,
        'lme_inv':   inv,
        'cancelled': cancel_pct,
        'recorder':  'akshare_bot',
        'source':    'lme_stock',
        'verified':  True,
        'flagged':   False,
    })

# 按日期去重
seen = set()
unique = []
for r in sorted(records, key=lambda x: x['date']):
    if r['date'] not in seen:
        seen.add(r['date'])
        unique.append(r)

print(f'  共 {len(unique)} 条，{unique[0]["date"]} ~ {unique[-1]["date"]}')
n = upsert_batch(unique, 'LME库存')
print(f'  ✓ 写入 {n} 条')

# ══════════════════════════════════════
# 2. LME 持仓（多空净仓位）
# ══════════════════════════════════════
print('[ 2/2 ] 拉取 LME 铜持仓...')
df2 = ak.macro_euro_lme_holding()

records2 = []
for _, row in df2.iterrows():
    date = str(row['日期'])[:10]
    records2.append({
        'date':      date,
        'lme_long':  safe_float(row.get('铜-多头仓位')),
        'lme_short': safe_float(row.get('铜-空头仓位')),
        'lme_net':   safe_float(row.get('铜-净仓位')),
        'recorder':  'akshare_bot',
        'source':    'lme_holding',
        'verified':  True,
        'flagged':   False,
    })

seen2 = set()
unique2 = []
for r in sorted(records2, key=lambda x: x['date']):
    if r['date'] not in seen2:
        seen2.add(r['date'])
        unique2.append(r)

print(f'  共 {len(unique2)} 条，{unique2[0]["date"]} ~ {unique2[-1]["date"]}')
n2 = upsert_batch(unique2, 'LME持仓')
print(f'  ✓ 写入 {n2} 条')

print('\n✅ 全部完成')
