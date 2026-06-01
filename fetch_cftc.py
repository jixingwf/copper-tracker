#!/usr/bin/env python3
"""
CFTC 铜持仓历史数据抓取 + 写入 Supabase
运行：python3 fetch_cftc.py
GitHub Actions：自动从环境变量读取 SUPABASE_URL / SUPABASE_KEY
"""

import os, requests, pandas as pd, time
from datetime import datetime
from io import StringIO

# ── 优先从环境变量读取，本地用 .env 文件 ──
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://opgqjxkaocggconjxgpi.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')   # 从环境变量读取，不硬编码

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def fetch_cftc():
    print("下载 CFTC Legacy COT 历史数据...")
    all_records = []

    urls = [
        ('近3年', 'https://www.cftc.gov/dea/newcot/deacot.txt'),
    ]

    for label, url in urls:
        try:
            print(f"  → {label}: {url}")
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            print(f"    状态码: {resp.status_code}, 大小: {len(resp.content)//1024} KB")

            lines = resp.text.split('\n')
            print(f"    总行数: {len(lines)}")
            for i, line in enumerate(lines[:3]):
                print(f"    样本行{i}: {line[:120]}")

            copper_lines = []
            for line in lines:
                upper = line.upper()
                if ('085692' in line or
                    ('COPPER' in upper and ('COMEX' in upper or 'CMX' in upper or 'NYMEX' in upper))):
                    copper_lines.append(line)

            print(f"    找到铜相关行: {len(copper_lines)}")
            if copper_lines:
                print(f"    铜数据样本: {copper_lines[0][:150]}")

            for line in copper_lines:
                fields = [f.strip().strip('"') for f in line.split(',')]
                if len(fields) < 10:
                    continue
                try:
                    raw_date = fields[2].strip()
                    dt = None
                    for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y', '%Y%m%d']:
                        try:
                            dt = datetime.strptime(raw_date, fmt).date()
                            break
                        except:
                            continue

                    if dt is None:
                        continue

                    def parse_int(s):
                        return int(s.replace(',', '').replace(' ', '')) if s.strip() else 0

                    long_pos  = parse_int(fields[6])
                    short_pos = parse_int(fields[7])
                    net_long  = long_pos - short_pos

                    if long_pos == 0 and short_pos == 0:
                        continue

                    all_records.append({
                        'date':        str(dt),
                        'cftc':        net_long,
                        'cftc_long':   long_pos,
                        'cftc_short':  short_pos,
                        'recorder':    'cftc_bot',
                        'source':      'cftc',
                        'verified':    True,
                        'flagged':     False,
                    })

                except Exception as e:
                    continue

            print(f"    解析成功: {len(all_records)} 条")
            time.sleep(1)

        except Exception as e:
            print(f"  ✗ 失败: {e}")

    if not all_records:
        print("\n⚠ 未获取到数据，尝试备用方式...")
        return fetch_cftc_csv_fallback()

    df = pd.DataFrame(all_records)
    df = df.drop_duplicates('date').sort_values('date')
    print(f"\n✓ 共获取 {len(df)} 条 CFTC 记录")
    print(df[['date','cftc','cftc_long','cftc_short']].tail(5).to_string())
    return df


def fetch_cftc_csv_fallback():
    import zipfile
    from io import BytesIO

    print("尝试备用下载方式（ZIP格式）...")
    url = 'https://www.cftc.gov/files/dea/history/fut_disagg_txt_2025.zip'
    all_records = []

    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()

        z = zipfile.ZipFile(BytesIO(resp.content))
        print(f"  ZIP内文件: {z.namelist()}")

        for fname in z.namelist():
            if fname.endswith('.txt') or fname.endswith('.csv'):
                content = z.read(fname).decode('utf-8', errors='ignore')
                lines = content.split('\n')

                for line in lines:
                    if 'COPPER' in line.upper() and ('COMEX' in line.upper() or '085692' in line):
                        fields = [f.strip().strip('"') for f in line.split(',')]
                        if len(fields) < 10:
                            continue
                        try:
                            raw_date = fields[2].strip()
                            dt = None
                            for fmt in ['%Y-%m-%d','%m/%d/%Y']:
                                try:
                                    dt = datetime.strptime(raw_date, fmt).date()
                                    break
                                except: continue
                            if not dt: continue

                            long_pos  = int(fields[8].replace(',',''))
                            short_pos = int(fields[9].replace(',',''))
                            net = long_pos - short_pos

                            all_records.append({
                                'date': str(dt),
                                'cftc': net,
                                'cftc_long': long_pos,
                                'cftc_short': short_pos,
                                'recorder': 'cftc_bot',
                                'source': 'cftc',
                                'verified': True,
                                'flagged': False,
                            })
                        except: continue

    except Exception as e:
        print(f"  ✗ 备用方式失败: {e}")

    if all_records:
        df = pd.DataFrame(all_records).drop_duplicates('date').sort_values('date')
        print(f"✓ 备用方式获取 {len(df)} 条")
        return df
    return None


def write_to_supabase(df):
    if df is None or len(df) == 0:
        print("没有数据可写入")
        return

    if not SUPABASE_KEY:
        print("\n⚠ 未配置 SUPABASE_KEY，保存为CSV")
        df.to_csv('cftc_data.csv', index=False, encoding='utf-8-sig')
        print("已保存到 cftc_data.csv")
        return

    print(f"\n写入 Supabase（{len(df)} 条）...")
    try:
        from supabase import create_client
        client = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"✗ 连接失败: {e}")
        df.to_csv('cftc_data.csv', index=False, encoding='utf-8-sig')
        return

    records = df.to_dict('records')
    success = 0

    for i in range(0, len(records), 50):
        batch = records[i:i+50]
        try:
            client.table('copper_records').upsert(
                batch, on_conflict='date,source'
            ).execute()
            success += len(batch)
            print(f"  已写入 {min(i+50, len(records))}/{len(records)} 条", end='\r')
        except Exception as e:
            print(f"\n  ✗ 第{i//50+1}批失败: {e}")
        time.sleep(0.2)

    print(f"\n✓ 写入完成：{success} 条")

    print("\n验证最新5条：")
    try:
        resp = client.table('copper_records')\
            .select('date,cftc,cftc_long,cftc_short')\
            .eq('source','cftc')\
            .order('date', desc=True)\
            .limit(5)\
            .execute()
        for r in resp.data:
            print(f"  {r['date']}  净多={r.get('cftc','—')}  多={r.get('cftc_long','—')}  空={r.get('cftc_short','—')}")
    except Exception as e:
        print(f"验证失败: {e}")


if __name__ == '__main__':
    df = fetch_cftc()
    write_to_supabase(df)
    print("\n✅ 完成")
