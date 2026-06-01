#!/usr/bin/env python3
"""
清理错误价格数据，重新写入正确值
运行：python3 fix_price_data.py
"""
import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except: pass

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://opgqjxkaocggconjxgpi.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')  # 填入你的 service_role key

if not SUPABASE_KEY:
    SUPABASE_KEY = input("请输入 Supabase service_role key: ").strip()

from supabase import create_client
client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 1. 查看当前 script source 的最近数据
print("当前 source=script 最近10条数据：")
resp = client.table('copper_records').select('id,date,lme,shfe,source,verified') \
    .eq('source', 'script').order('date', desc=True).limit(10).execute()
for r in resp.data:
    print(f"  {r['date']}  lme={r.get('lme')}  shfe={r.get('shfe')}  verified={r.get('verified')}")

# 2. 找出 lme > 50000 的明显错误数据（正常铜价 4000-15000 $/吨）
print("\n查找异常数据（lme > 15000）：")
resp2 = client.table('copper_records').select('id,date,lme,shfe') \
    .eq('source', 'script').gt('lme', 15000).execute()
bad_ids = [r['id'] for r in resp2.data]
print(f"  找到 {len(bad_ids)} 条异常记录")
for r in resp2.data:
    print(f"  id={r['id']}  date={r['date']}  lme={r.get('lme')}")

# 3. 删除异常数据
if bad_ids:
    confirm = input(f"\n确认删除这 {len(bad_ids)} 条异常数据？(y/n): ")
    if confirm.lower() == 'y':
        for bid in bad_ids:
            client.table('copper_records').delete().eq('id', bid).execute()
        print(f"✓ 已删除 {len(bad_ids)} 条")
    else:
        print("已取消")
else:
    print("  没有异常数据，无需清理")

# 4. 检查 verified=False 的数据
print("\n查找 verified=False 的数据（前10条）：")
resp3 = client.table('copper_records').select('id,date,lme,source,verified') \
    .eq('verified', False).order('date', desc=True).limit(10).execute()
print(f"  共找到记录（前10）：{len(resp3.data)} 条")
for r in resp3.data:
    print(f"  id={r['id']}  date={r['date']}  source={r['source']}  lme={r.get('lme')}")

if resp3.data:
    confirm2 = input(f"\n将所有 verified=False 的 source=script 数据改为 verified=True？(y/n): ")
    if confirm2.lower() == 'y':
        client.table('copper_records').update({'verified': True}) \
            .eq('source', 'script').eq('verified', False).execute()
        print("✓ 已更新")

print("\n✅ 完成")
