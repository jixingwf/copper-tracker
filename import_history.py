#!/usr/bin/env python3
"""
铜历史数据下载 + 导入 Supabase
================================
数据来源：
  - COMEX铜期货价格  → yfinance（免费，5年历史）
  - CFTC持仓报告     → cftc.gov 官方CSV（免费，全部历史）

在你的电脑上运行：
  pip install yfinance pandas requests supabase python-dotenv
  python3 import_history.py

会生成两个本地CSV文件（可用Excel查看）并导入Supabase。
"""

import os, time, requests
import pandas as pd
from datetime import datetime, date
from io import StringIO

# ── 配置 ─────────────────────────────────────────────────
# 直接填在这里，或者从 .env 文件读取
SUPABASE_URL = 'https://opgqjxkaocggconjxgpi.supabase.co'
SUPABASE_KEY = 'sb_secret_wMEiaD2wNjcwYeOD7fPagQ_SlEI8jqj'   # ← 填 service_role key（不是anon key）

# 历史数据年限（COMEX价格）
YEARS_BACK = 5   # 可改为 2、3、10

# ── 1. 下载 COMEX 铜价历史 ────────────────────────────────
def fetch_comex_history():
    print("\n[1/3] 下载 COMEX 铜期货历史价格（via yfinance）...")
    try:
        import yfinance as yf
        ticker = yf.Ticker("HG=F")
        hist = ticker.history(period=f"{YEARS_BACK}y")

        if hist.empty:
            print("  ✗ 未获取到数据")
            return None

        df = hist[['Open','High','Low','Close','Volume']].copy()
        df.index = pd.to_datetime(df.index).date
        df.index.name = 'date'
        df.columns = ['open','high','low','close','volume']

        # 换算：HG以美分/磅 → 美元/吨
        for col in ['open','high','low','close']:
            df[col+'_usd_ton'] = (df[col] / 100 * 2204.62).round(0)

        df['close_raw'] = df['close'].round(4)   # 原始价格（美分/磅）
        df['lme_equiv'] = df['close_usd_ton']     # 近似LME等价（仅参考）

        # 过滤掉节假日空数据
        df = df[df['close'] > 0]

        print(f"  ✓ 获取到 {len(df)} 条记录（{df.index[0]} ~ {df.index[-1]}）")
        return df

    except ImportError:
        print("  ✗ 请先安装：pip install yfinance")
        return None
    except Exception as e:
        print(f"  ✗ 错误：{e}")
        return None


# ── 2. 下载 CFTC 持仓历史 ────────────────────────────────
def fetch_cftc_history():
    print("\n[2/3] 下载 CFTC 铜持仓历史（via cftc.gov）...")

    # CFTC提供历年历史数据压缩包，格式为Disaggregated COT
    # 近3年数据在一个文件里，历史数据按年分开
    all_frames = []

    # 近3年（合并文件）
    urls = {
        '近3年': 'https://www.cftc.gov/dea/newcot/f_disagg.txt',
    }

    # 可选：加载更早年份（每年一个文件）
    # for year in range(2015, 2023):
    #     urls[str(year)] = f'https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip'

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }

    for label, url in urls.items():
        try:
            print(f"  → 下载 {label}...")
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()

            lines = resp.text.split('\n')
            copper_lines = [l for l in lines if 'COPPER' in l.upper() and 'COMEX' in l.upper()]

            if not copper_lines:
                print(f"    ✗ 未找到铜数据")
                continue

            records = []
            for line in copper_lines:
                fields = [f.strip().strip('"') for f in line.split(',')]
                if len(fields) < 15:
                    continue
                try:
                    # Disaggregated COT 字段说明：
                    # 0: Market_and_Exchange_Names
                    # 2: Report_Date_as_YYYY-MM-DD
                    # 8: NonComm_Positions_Long_All
                    # 9: NonComm_Positions_Short_All
                    # 10: NonComm_Postions_Spread_All
                    report_date = fields[2]
                    if not report_date or len(report_date) < 8:
                        continue

                    long_pos  = int(fields[8].replace(',',''))
                    short_pos = int(fields[9].replace(',',''))
                    spread    = int(fields[10].replace(',','')) if fields[10].replace(',','').strip() else 0
                    net_long  = long_pos - short_pos

                    # 标准化日期格式
                    try:
                        dt = datetime.strptime(report_date, '%Y-%m-%d').date()
                    except:
                        try:
                            dt = datetime.strptime(report_date, '%m/%d/%Y').date()
                        except:
                            continue

                    records.append({
                        'date':         dt,
                        'cftc_long':    long_pos,
                        'cftc_short':   short_pos,
                        'cftc_spread':  spread,
                        'cftc_net':     net_long,
                    })
                except (ValueError, IndexError):
                    continue

            if records:
                frame = pd.DataFrame(records)
                all_frames.append(frame)
                print(f"    ✓ {len(records)} 条")

            time.sleep(1)

        except Exception as e:
            print(f"    ✗ 下载失败：{e}")

    if not all_frames:
        return None

    df = pd.concat(all_frames).drop_duplicates('date').sort_values('date')
    df = df.set_index('date')
    print(f"  ✓ CFTC共 {len(df)} 条记录（{df.index[0]} ~ {df.index[-1]}）")
    return df


# ── 3. 合并并保存CSV ─────────────────────────────────────
def merge_and_save(comex_df, cftc_df):
    print("\n[3/3] 合并数据...")

    frames = []
    if comex_df is not None:
        frames.append(comex_df[['close_raw','lme_equiv']])
    if cftc_df is not None:
        frames.append(cftc_df)

    if not frames:
        print("  ✗ 没有可用数据")
        return None

    if len(frames) == 2:
        merged = pd.merge(
            comex_df[['close_raw','lme_equiv']],
            cftc_df,
            left_index=True, right_index=True,
            how='outer'
        )
    else:
        merged = frames[0]

    merged.index = pd.to_datetime(merged.index)
    merged = merged.sort_index()
    merged.index.name = 'date'

    # 保存CSV（Excel可直接打开）
    comex_path = 'copper_comex_history.csv'
    cftc_path  = 'copper_cftc_history.csv'
    merged_path = 'copper_history_merged.csv'

    if comex_df is not None:
        comex_df.to_csv(comex_path, encoding='utf-8-sig')
        print(f"  ✓ COMEX数据 → {comex_path}")

    if cftc_df is not None:
        cftc_df.to_csv(cftc_path, encoding='utf-8-sig')
        print(f"  ✓ CFTC数据  → {cftc_path}")

    merged.to_csv(merged_path, encoding='utf-8-sig')
    print(f"  ✓ 合并数据  → {merged_path}（{len(merged)} 行）")

    return merged


# ── 4. 导入 Supabase ─────────────────────────────────────
def import_to_supabase(merged_df):
    if not SUPABASE_URL or 'YOUR_SERVICE_ROLE_KEY' in SUPABASE_KEY:
        print("\n⚠  未配置 Supabase，跳过导入（CSV已保存，可手动导入）")
        print("   在脚本顶部填入 SUPABASE_URL 和 SUPABASE_KEY 后重新运行")
        return

    print(f"\n[4/4] 导入 Supabase（共 {len(merged_df)} 条）...")

    try:
        from supabase import create_client
        client = create_client(SUPABASE_URL, SUPABASE_KEY)
    except ImportError:
        print("  ✗ 请安装：pip install supabase")
        return
    except Exception as e:
        print(f"  ✗ Supabase连接失败：{e}")
        return

    # 构建插入记录
    records = []
    for dt, row in merged_df.iterrows():
        r = {
            'date':     str(dt.date() if hasattr(dt,'date') else dt)[:10],
            'recorder': 'history_import',
            'source':   'script',
            'verified': True,   # 历史数据直接标记为已验证
            'flagged':  False,
        }
        # COMEX价格（换算为近似LME美元/吨）
        if 'lme_equiv' in row and pd.notna(row['lme_equiv']):
            r['lme'] = float(row['lme_equiv'])
        # CFTC
        if 'cftc_net' in row and pd.notna(row['cftc_net']):
            r['cftc']       = int(row['cftc_net'])
        if 'cftc_long' in row and pd.notna(row['cftc_long']):
            r['cftc_long']  = int(row['cftc_long'])
        if 'cftc_short' in row and pd.notna(row['cftc_short']):
            r['cftc_short'] = int(row['cftc_short'])

        records.append(r)

    # 分批插入（每批100条，避免超时）
    batch_size = 100
    success = 0
    fail = 0

    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        try:
            resp = client.table('copper_records').upsert(
                batch,
                on_conflict='date,source'   # 同日期+来源的记录更新而非重复插入
            ).execute()
            success += len(batch)
            print(f"  → 已导入 {min(i+batch_size, len(records))}/{len(records)} 条", end='\r')
        except Exception as e:
            fail += len(batch)
            print(f"\n  ✗ 第{i//batch_size+1}批失败：{e}")
        time.sleep(0.2)

    print(f"\n  ✓ 导入完成：成功 {success} 条，失败 {fail} 条")


# ── 主流程 ────────────────────────────────────────────────
if __name__ == '__main__':
    print("="*55)
    print("  铜历史数据下载 + 导入 Supabase")
    print("="*55)

    comex_df = fetch_comex_history()
    cftc_df  = fetch_cftc_history()
    merged   = merge_and_save(comex_df, cftc_df)

    if merged is not None:
        import_to_supabase(merged)

    print("\n✅ 完成")
    print("\n生成文件：")
    for f in ['copper_comex_history.csv','copper_cftc_history.csv','copper_history_merged.csv']:
        if os.path.exists(f):
            size = os.path.getsize(f) // 1024
            print(f"  {f}  ({size} KB)")
