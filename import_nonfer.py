#!/usr/bin/env python3
"""
有色金属历史数据导入脚本
铝(AL) / 锌(ZN) / 镍(NI) / 铅(PB) / 锡(SN)

数据来源：
  - SHFE价格：新浪期货主力合约（futures_main_sina）
  - SHFE库存：东方财富（futures_inventory_em）
  - LME价格：新浪LME指数（futures_lme_index_symbol_table_sina）
  - LME库存+持仓：AkShare（macro_euro_lme_stock / macro_euro_lme_holding）

运行：python3 import_nonfer.py
"""

import os, time, math
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

# 五个品种配置
METALS = {
    'al': {'name':'铝', 'sina':'AL0', 'inv99':'铝', 'inv_em':'沪铝', 'lme_col':'铝', 'unit_lme':1000, 'lme_sina':'铝'},
    'zn': {'name':'锌', 'sina':'ZN0', 'inv99':'锌', 'inv_em':'沪锌', 'lme_col':'锌', 'unit_lme':25,   'lme_sina':'锌'},
    'ni': {'name':'镍', 'sina':'NI0', 'inv99':'镍', 'inv_em':'镍',   'lme_col':'镍', 'unit_lme':6,    'lme_sina':'镍'},
    'pb': {'name':'铅', 'sina':'PB0', 'inv99':'铅', 'inv_em':'沪铅', 'lme_col':'铅', 'unit_lme':25,   'lme_sina':'铅'},
    'sn': {'name':'锡', 'sina':'SN0', 'inv99':'锡', 'inv_em':'锡',   'lme_col':'锡', 'unit_lme':5,    'lme_sina':'锡'},
}

def safe_float(v):
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except:
        return None

METAL_TABLE_MAP = {
    'al': 'aluminum_records',
    'zn': 'zinc_records',
    'ni': 'nickel_records',
    'pb': 'lead_records',
    'sn': 'tin_records',
}

def upsert(client, records, label):
    if not records:
        print(f"    {label}: 无数据")
        return
    # 写入主表 metal_records
    for i in range(0, len(records), 100):
        batch = records[i:i+100]
        try:
            client.table('metal_records').upsert(
                batch, on_conflict='metal,date,source'
            ).execute()
            print(f"    {label}: 写入 {min(i+100,len(records))}/{len(records)} 条", end='\r')
        except Exception as e:
            print(f"\n    {label} 批次失败: {e}")
        time.sleep(0.15)
    print(f"    {label}: ✓ {len(records)} 条")
    # 同步写入各金属独立表
    by_metal = {}
    for r in records:
        m = r.get('metal')
        if m in METAL_TABLE_MAP:
            by_metal.setdefault(m, []).append(r)
    for metal_id, metal_records in by_metal.items():
        table = METAL_TABLE_MAP[metal_id]
        for i in range(0, len(metal_records), 100):
            batch = metal_records[i:i+100]
            try:
                client.table(table).upsert(
                    batch, on_conflict='metal,date,source'
                ).execute()
            except Exception as e:
                print(f"\n    {label} → {table} 批次失败: {e}")
            time.sleep(0.1)

# ════════════════════════════════════════════
# 0. LME 价格（新浪LME指数，一次拉所有品种）
# ════════════════════════════════════════════
def import_lme_price(client):
    print("\n  [全部] LME价格（一次拉取所有品种）...")
    # 新浪LME品种代码映射
    lme_symbols = {
        'al': 'AHD',  # 铝
        'zn': 'ZSD',  # 锌
        'ni': 'NID',  # 镍
        'pb': 'PBD',  # 铅
        'sn': 'SND',  # 锡
    }
    for metal_id, symbol in lme_symbols.items():
        name = METALS[metal_id]['name']
        print(f"  [{name}] LME价格 ({symbol})...")
        try:
            # FIX: akshare已移除 futures_lme_index_symbol_table_sina，
            # 改用 futures_foreign_hist（同样的新浪外盘代码 AHD/ZSD/NID/PBD/SND）
            df = ak.futures_foreign_hist(symbol=symbol)
            if df is None or df.empty:
                print(f"    [{name}] 空数据，跳过")
                continue
            print(f"    字段: {df.columns.tolist()}")
            records = []
            for _, row in df.iterrows():
                # futures_foreign_hist 返回字段通常为 date/open/high/low/close/volume
                date_val = row.get('date') or row.get('日期') or row.get(df.columns[0])
                date = str(date_val)[:10]
                price = safe_float(row.get('close') or row.get('收盘价') or row.get('收盘'))
                if not price or not date or date == 'nan':
                    continue
                records.append({
                    'metal':     metal_id,
                    'date':      date,
                    'source':    'lme_price',  # FIX: 统一用 lme_price，前端按此source读取
                    'lme_price': price,
                    'verified':  True,
                    'flagged':   False,
                })
            seen, unique = set(), []
            for r in sorted(records, key=lambda x: x['date']):
                k = (r['metal'], r['date'], r['source'])
                if k not in seen:
                    seen.add(k)
                    unique.append(r)
            upsert(client, unique, f"{name}LME价格")
            time.sleep(1)
        except Exception as e:
            print(f"    [{name}] LME价格失败: {e}")

# ════════════════════════════════════════════
# 1. SHFE 价格（新浪期货主力）
# ════════════════════════════════════════════
def import_shfe_price(client, metal_id, cfg):
    print(f"  [{cfg['name']}] SHFE价格...")
    try:
        df = ak.futures_main_sina(symbol=cfg['sina'])
        records = []
        for _, row in df.iterrows():
            date = str(row['日期'])[:10]
            price = safe_float(row.get('收盘价'))
            if not price:
                continue
            records.append({
                'metal':      metal_id,
                'date':       date,
                'source':     'shfe_price',
                'shfe_price': price,
                'verified':   True,
                'flagged':    False,
            })
        # 去重
        seen, unique = set(), []
        for r in sorted(records, key=lambda x: x['date']):
            k = (r['metal'], r['date'], r['source'])
            if k not in seen:
                seen.add(k)
                unique.append(r)
        upsert(client, unique, f"{cfg['name']}SHFE价格")
        time.sleep(1)
    except Exception as e:
        print(f"    [{cfg['name']}] SHFE价格失败: {e}")

# ════════════════════════════════════════════
# 2. SHFE 库存（99期货网）
# ════════════════════════════════════════════
def import_shfe_inv(client, metal_id, cfg):
    print(f"  [{cfg['name']}] SHFE库存...")
    try:
        df = ak.futures_inventory_em(symbol=cfg["inv_em"])
        records = []
        for _, row in df.iterrows():
            date = str(row['日期'])[:10]
            inv = safe_float(row.get('库存'))
            if not inv:
                continue
            records.append({
                'metal':    metal_id,
                'date':     date,
                'source':   'shfe_inv_99' if metal_id == 'cu' else 'shfe_stock',
                'shfe_inv': inv,
                'verified': True,
                'flagged':  False,
            })
        seen, unique = set(), []
        for r in sorted(records, key=lambda x: x['date']):
            k = (r['metal'], r['date'], r['source'])
            if k not in seen:
                seen.add(k)
                unique.append(r)
        upsert(client, unique, f"{cfg['name']}SHFE库存")
        time.sleep(1)
    except Exception as e:
        print(f"    [{cfg['name']}] SHFE库存失败: {e}")

# ════════════════════════════════════════════
# 3. LME 库存（一次性拉所有品种）
# ════════════════════════════════════════════
def import_lme_stock(client):
    print("\n  [全部] LME库存（一次拉取所有品种）...")
    try:
        df = ak.macro_euro_lme_stock()
        print(f"    字段: {df.columns.tolist()}")

        # 字段映射：品种 -> (库存列, 注册仓单列, 注销仓单列)
        col_map = {
            'al': ('铝-库存', '铝-注册仓单', '铝-注销仓单'),
            'zn': ('锌-库存', '锌-注册仓单', '锌-注销仓单'),
            'ni': ('镍-库存', '镍-注册仓单', '镍-注销仓单'),
            'pb': ('铅-库存', '铅-注册仓单', '铅-注销仓单'),
            'sn': ('锡-库存', '锡-注册仓单', '锡-注销仓单'),
        }

        all_records = []
        for metal_id, (inv_col, warrant_col, cancel_col) in col_map.items():
            if inv_col not in df.columns:
                print(f"    [{metal_id}] 找不到列 {inv_col}")
                continue
            for _, row in df.iterrows():
                date = str(row['日期'])[:10]
                inv     = safe_float(row.get(inv_col))
                warrant = safe_float(row.get(warrant_col))
                cancel  = safe_float(row.get(cancel_col))
                if inv is None:
                    continue
                all_records.append({
                    'metal':      metal_id,
                    'date':       date,
                    'source':     'lme_stock',
                    'lme_inv':    inv,
                    'lme_warrant':warrant,
                    'lme_cancel': cancel,
                    'verified':   True,
                    'flagged':    False,
                })

        # 按品种去重
        seen, unique = set(), []
        for r in sorted(all_records, key=lambda x: (x['metal'], x['date'])):
            k = (r['metal'], r['date'], r['source'])
            if k not in seen:
                seen.add(k)
                unique.append(r)

        print(f"    共 {len(unique)} 条（5个品种）")
        upsert(client, unique, "LME库存")
        time.sleep(1)
    except Exception as e:
        print(f"    LME库存失败: {e}")

# ════════════════════════════════════════════
# 4. LME 持仓（一次性拉所有品种）
# ════════════════════════════════════════════
def import_lme_holding(client):
    print("\n  [全部] LME持仓（一次拉取所有品种）...")
    try:
        df = ak.macro_euro_lme_holding()
        print(f"    字段: {df.columns.tolist()}")

        col_map = {
            'al': ('铝-多头仓位', '铝-空头仓位', '铝-净仓位'),
            'zn': ('锌-多头仓位', '锌-空头仓位', '锌-净仓位'),
            'ni': ('镍-多头仓位', '镍-空头仓位', '镍-净仓位'),
            'pb': ('铅-多头仓位', '铅-空头仓位', '铅-净仓位'),
            'sn': ('锡-多头仓位', '锡-空头仓位', '锡-净仓位'),
        }

        all_records = []
        for metal_id, (long_col, short_col, net_col) in col_map.items():
            if long_col not in df.columns:
                print(f"    [{metal_id}] 找不到列 {long_col}")
                continue
            for _, row in df.iterrows():
                date  = str(row['日期'])[:10]
                long_ = safe_float(row.get(long_col))
                short_= safe_float(row.get(short_col))
                net   = safe_float(row.get(net_col))
                if long_ is None:
                    continue
                all_records.append({
                    'metal':     metal_id,
                    'date':      date,
                    'source':    'lme_holding',
                    'lme_long':  long_,
                    'lme_short': short_,
                    'lme_net':   net,
                    'verified':  True,
                    'flagged':   False,
                })

        seen, unique = set(), []
        for r in sorted(all_records, key=lambda x: (x['metal'], x['date'])):
            k = (r['metal'], r['date'], r['source'])
            if k not in seen:
                seen.add(k)
                unique.append(r)

        print(f"    共 {len(unique)} 条（5个品种）")
        upsert(client, unique, "LME持仓")
        time.sleep(1)
    except Exception as e:
        print(f"    LME持仓失败: {e}")

# ════════════════════════════════════════════
# 主流程
# ════════════════════════════════════════════
if __name__ == '__main__':
    print("="*55)
    print("  有色金属历史数据导入")
    print("  铝 / 锌 / 镍 / 铅 / 锡")
    print("="*55)

    if 'YOUR_SECRET_KEY' in SUPABASE_KEY:
        print("⚠  请先填写 SUPABASE_KEY")
        exit(1)

    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # LME 价格（一次拉所有）
    import_lme_price(client)

    # SHFE 价格（每个品种单独拉）
    print("\n【SHFE 价格】")
    for metal_id, cfg in METALS.items():
        import_shfe_price(client, metal_id, cfg)

    # SHFE 库存（每个品种单独拉）
    print("\n【SHFE 库存】")
    for metal_id, cfg in METALS.items():
        import_shfe_inv(client, metal_id, cfg)

    # LME 库存（一次拉所有）
    import_lme_stock(client)

    # LME 持仓（一次拉所有）
    import_lme_holding(client)

    # 验证结果
    print("\n【验证写入结果】")
    try:
        resp = client.table('metal_records')\
            .select('metal, source', count='exact')\
            .execute()
        from collections import Counter
        counts = Counter((r['metal'], r['source']) for r in resp.data)
        for (metal, source), cnt in sorted(counts.items()):
            print(f"  {metal:4s} {source:15s}: {cnt} 条")
    except Exception as e:
        print(f"  验证失败: {e}")

    print("\n✅ 全部完成")
