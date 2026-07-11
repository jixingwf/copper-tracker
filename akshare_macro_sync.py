#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
中国宏观经济数据自动抓取脚本
============================
使用 AkShare（免费、无需 API Key，聚合国家统计局/央行/海关总署等官方数据）
抓取 CPI / PPI / PMI / M0-M2 / 社融 / 进出口 / 社零 / 工业增加值 / 固定资产投资 /
房地产 / 用电量 / 财政 等指标，写入 Supabase 的 macro_indicators 表。

使用前准备：
    pip install akshare pandas supabase --break-system-packages

运行：
    python akshare_macro_sync.py            # 抓取全部指标（增量更新，自动去重）
    python akshare_macro_sync.py --only cpi ppi pmi   # 只更新指定分组

定时自动更新（任选其一，本脚本自身不包含调度器）：
    1. Linux/Mac crontab，例如每天早上7点跑一次：
         0 7 * * * cd /path/to/project && /usr/bin/python3 akshare_macro_sync.py >> sync.log 2>&1
    2. Windows 任务计划程序，新建基本任务，程序填 python，参数填本文件路径。
    3. GitHub Actions（推荐，免服务器）：在仓库 .github/workflows/macro_sync.yml 中
       配置 schedule cron 触发，Secrets 中存 SUPABASE_URL / SUPABASE_KEY，
       workflow 里 `pip install -r requirements.txt && python akshare_macro_sync.py`。

注意：
    - 部分指标（工业企业利润、汽车产销、部分房地产细项）AkShare 暂无稳定接口，
      请通过 macro_entry.html 页面手动录入，或在下方 TODO 处按需补充数据源。
    - 若指标接口返回字段名称随 AkShare 版本变化，请对照 akshare 官方文档
      https://akshare.akfamily.xyz/data/macro/macro.html 调整解析逻辑。
"""

import argparse
import re
import sys
import time
from datetime import datetime, timezone

import pandas as pd

try:
    import akshare as ak
except ImportError:
    print("请先安装 akshare: pip install akshare --break-system-packages")
    sys.exit(1)

try:
    from supabase import create_client
except ImportError:
    print("请先安装 supabase: pip install supabase --break-system-packages")
    sys.exit(1)

import os

# ════════════════════════════════
# 配置区：请替换为你自己的 Supabase 项目信息
# （与前端 macro.html / macro_detail.html 中的 SUPABASE_URL 保持一致）
#
# 优先从环境变量 SUPABASE_URL / SUPABASE_KEY 读取（GitHub Actions 中通过
# Secrets 注入，避免密钥明文提交到仓库）；本地未设置环境变量时，回退到
# 下面写死的默认值，方便本地直接运行。
# ════════════════════════════════
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://opgqjxkaocggconjxgpi.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "sb_publishable_LiPH9UV6-Y_vYGbMpYZ-fw_k0un32VC")  # 建议改用 service_role key 以便脚本稳定写入

sb = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_with_retry(func, *args, retries=3, delay=5, **kwargs):
    """带自动重试的 AkShare 接口调用封装。

    国内数据源（东方财富/金十数据等）偶尔会因为网络抖动、被限速等原因导致
    连接中途断开（例如 "Response ended prematurely"），尤其是从 GitHub
    Actions 这类境外服务器访问时更容易出现。这类问题通常隔几秒重试一两次
    就能成功，不需要让整个分组直接判定失败。
    """
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_err = e
            if attempt < retries:
                print(f"    第 {attempt} 次请求失败（{e}），{delay} 秒后重试...")
                time.sleep(delay)
    raise last_err


def upsert_rows(rows):
    """批量写入 macro_indicators，按 series_code + period 去重覆盖"""
    if not rows:
        return
    # 同一批数据里可能出现同一个 series_code+period 被多次抓到（例如数据修正后
    # 接口返回了新旧两条记录），Supabase 的 upsert 不允许在同一条 SQL 命令里
    # 对同一行做两次更新，这里先按 key 去重，同 key 保留后出现的那一条（通常是
    # 更新的数据）。
    dedup: dict[tuple, dict] = {}
    for row in rows:
        key = (row["series_code"], row["period"])
        dedup[key] = row
    rows = list(dedup.values())

    # 分批写入，避免单次请求过大
    batch = 200
    for i in range(0, len(rows), batch):
        chunk = rows[i:i + batch]
        sb.table("macro_indicators").upsert(chunk, on_conflict="series_code,period").execute()
    print(f"  写入 {len(rows)} 条记录")


def month_to_period(dt) -> tuple[str, str]:
    """把各种日期表示统一转成 (period 'YYYY-MM-01', period_label 'YYYY-MM')

    AkShare 不同接口返回的日期字符串格式很不统一，常见的有：
        - "2026年06月份" / "2026年6月"（国家统计局系接口，如 CPI/PPI/PMI/社零/财政）
        - "201501"（纯数字 YYYYMM，如社融 macro_china_shrzgm）
        - "2004.11" / "2004.3"（点号分隔，月份不补零，如用电量接口）
        - "2026-06-01" / "2026-06" / 标准日期字符串（如出口/进口同比等金十系接口）
    这里统一做正则预处理，解析不出来的格式再交给 pandas 兜底。
    """
    s = str(dt).strip()

    # 纯数字 YYYYMM，例如社融接口 "201501"
    m = re.fullmatch(r"(\d{4})(\d{2})", s)
    if m:
        year, month = int(m.group(1)), int(m.group(2))
        return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}"

    # 中文格式，例如 "2026年06月份" / "2026年6月"
    m = re.match(r"(\d{4})年(\d{1,2})月", s)
    if m:
        year, month = int(m.group(1)), int(m.group(2))
        return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}"

    # 点号分隔，例如用电量接口 "2004.11" / "2004.3"
    m = re.match(r"(\d{4})\.(\d{1,2})$", s)
    if m:
        year, month = int(m.group(1)), int(m.group(2))
        return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}"

    # 其余情况交给 pandas 解析（如 "2026-06-01"、"2026-06" 等标准/近标准格式）
    ts = pd.to_datetime(s)
    return ts.strftime("%Y-%m-01"), ts.strftime("%Y-%m")


def build_row(category, series_code, series_name, dt, value, unit, yoy=None, mom=None, source=None):
    period, label = month_to_period(dt)
    return {
        "category": category,
        "series_code": series_code,
        "series_name": series_name,
        "period": period,
        "period_label": label,
        "value": None if pd.isna(value) else float(value),
        "unit": unit,
        "yoy": None if (yoy is None or pd.isna(yoy)) else float(yoy),
        "mom": None if (mom is None or pd.isna(mom)) else float(mom),
        "source": source,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


# ════════════════════════════════
# 各分组抓取函数
# 每个函数返回 rows: List[dict]，抓取失败时打印警告并返回空列表，不中断整体流程
# ════════════════════════════════

def sync_price():
    rows = []
    try:
        df = fetch_with_retry(ak.macro_china_cpi)  # 全国居民消费价格指数
        # 字段名可能随版本变化，请以 print(df.columns) 实际输出为准微调
        for _, r in df.iterrows():
            rows.append(build_row("price", "cpi_yoy", "CPI 同比", r["月份"],
                                   r.get("全国-同比增长"), "%", source="国家统计局"))
            rows.append(build_row("price", "cpi_mom", "CPI 环比", r["月份"],
                                   r.get("全国-环比增长"), "%", source="国家统计局"))
    except Exception as e:
        print(f"  [警告] CPI 抓取失败: {e}")

    try:
        df = fetch_with_retry(ak.macro_china_ppi)  # 工业生产者出厂价格指数
        for _, r in df.iterrows():
            rows.append(build_row("price", "ppi_yoy", "PPI 同比", r["月份"],
                                   r.get("PPI-全部工业品-当月同比"), "%", source="国家统计局"))
    except Exception as e:
        print(f"  [警告] PPI 抓取失败: {e}")

    return rows


def sync_pmi():
    rows = []
    try:
        df = fetch_with_retry(ak.macro_china_pmi)  # 官方制造业/非制造业PMI
        for _, r in df.iterrows():
            rows.append(build_row("pmi", "pmi_mfg", "官方制造业PMI", r["月份"],
                                   r.get("制造业-指数"), "指数", source="国家统计局"))
            rows.append(build_row("pmi", "pmi_nonmfg", "官方非制造业PMI", r["月份"],
                                   r.get("非制造业-指数"), "指数", source="国家统计局"))
    except Exception as e:
        print(f"  [警告] 官方PMI 抓取失败: {e}")

    try:
        df = fetch_with_retry(ak.macro_china_cx_pmi_yearly)  # 财新制造业PMI终值（金十数据）
        for _, r in df.iterrows():
            rows.append(build_row("pmi", "caixin_pmi_mfg", "财新制造业PMI", r["日期"],
                                   r.get("今值"), "指数", source="财新/Markit"))
    except Exception as e:
        print(f"  [警告] 财新制造业PMI 抓取失败（接口名可能需调整）: {e}")

    try:
        df = fetch_with_retry(ak.macro_china_cx_services_pmi_yearly)  # 财新服务业PMI（金十数据）
        for _, r in df.iterrows():
            rows.append(build_row("pmi", "caixin_pmi_services", "财新服务业PMI", r["日期"],
                                   r.get("今值"), "指数", source="财新/Markit"))
    except Exception as e:
        print(f"  [警告] 财新服务业PMI 抓取失败（接口名可能需调整）: {e}")

    return rows


def sync_money():
    rows = []
    try:
        df = fetch_with_retry(ak.macro_china_money_supply)  # M0/M1/M2
        for _, r in df.iterrows():
            rows.append(build_row("money", "m0_yoy", "M0 同比", r["月份"],
                                   r.get("货币和准货币(M2)-同比增长"), "%", source="央行"))
            rows.append(build_row("money", "m1_yoy", "M1 同比", r["月份"],
                                   r.get("货币(M1)-同比增长"), "%", source="央行"))
            rows.append(build_row("money", "m2_yoy", "M2 同比", r["月份"],
                                   r.get("货币和准货币(M2)-同比增长"), "%", source="央行"))
    except Exception as e:
        print(f"  [警告] M0/M1/M2 抓取失败: {e}")

    try:
        df = fetch_with_retry(ak.macro_china_shrzgm)  # 社会融资规模增量
        for _, r in df.iterrows():
            rows.append(build_row("money", "shrzgm_increment", "社会融资规模增量", r["月份"],
                                   r.get("社会融资规模增量"), "亿元", source="央行"))
    except Exception as e:
        print(f"  [警告] 社融 抓取失败: {e}")

    return rows


def sync_trade():
    rows = []
    try:
        df = fetch_with_retry(ak.macro_china_exports_yoy)  # 字段: 商品/日期/今值/预测值/前值
        for _, r in df.iterrows():
            if pd.isna(r.get("今值")):
                continue
            rows.append(build_row("trade", "exports_yoy", "出口 同比", r["日期"],
                                   r.get("今值"), "%", source="海关总署"))
    except Exception as e:
        print(f"  [警告] 出口同比 抓取失败: {e}")

    try:
        df = fetch_with_retry(ak.macro_china_imports_yoy)  # 字段: 商品/日期/今值/预测值/前值
        for _, r in df.iterrows():
            if pd.isna(r.get("今值")):
                continue
            rows.append(build_row("trade", "imports_yoy", "进口 同比", r["日期"],
                                   r.get("今值"), "%", source="海关总署"))
    except Exception as e:
        print(f"  [警告] 进口同比 抓取失败: {e}")

    return rows


def sync_retail():
    rows = []
    try:
        df = fetch_with_retry(ak.macro_china_consumer_goods_retail)
        for _, r in df.iterrows():
            rows.append(build_row("retail", "retail_yoy", "社零 同比", r["月份"],
                                   r.get("当月同比增长"), "%", source="国家统计局"))
    except Exception as e:
        print(f"  [警告] 社会消费品零售总额 抓取失败: {e}")
    return rows


def sync_electricity():
    rows = []
    try:
        df = fetch_with_retry(ak.macro_china_society_electricity)  # 字段: 统计时间/全社会用电量/全社会用电量同比/...
        for _, r in df.iterrows():
            if pd.isna(r.get("全社会用电量同比")):
                continue
            rows.append(build_row("electricity", "electricity_yoy", "全社会用电量 同比", r["统计时间"],
                                   r.get("全社会用电量同比"), "%", source="国家能源局"))
    except Exception as e:
        print(f"  [警告] 用电量 抓取失败（接口名可能需调整）: {e}")
    return rows


def sync_fiscal():
    rows = []
    try:
        df = fetch_with_retry(ak.macro_china_czsr)  # 财政收入
        for _, r in df.iterrows():
            rows.append(build_row("fiscal", "fiscal_revenue_yoy", "财政收入 累计同比", r["月份"],
                                   r.get("当月同比增长"), "%", source="财政部"))
    except Exception as e:
        print(f"  [警告] 财政收入 抓取失败: {e}")
    return rows


GROUPS = {
    "price": sync_price,
    "pmi": sync_pmi,
    "money": sync_money,
    "trade": sync_trade,
    "retail": sync_retail,
    "electricity": sync_electricity,
    "fiscal": sync_fiscal,
    # TODO: investment（固定资产投资）、realestate（房地产）需要的接口在不同 akshare
    # 版本间差异较大，建议实际运行前打印 dir(ak) 里含 "china" 的函数名核对后再接入；
    # auto（汽车产销）、industry 中的工业企业利润 目前建议用 macro_entry.html 手动录入。
}


def main():
    parser = argparse.ArgumentParser(description="中国宏观经济数据同步脚本")
    parser.add_argument("--only", nargs="*", choices=list(GROUPS.keys()),
                         help="只同步指定分组，默认同步全部")
    args = parser.parse_args()

    targets = args.only if args.only else list(GROUPS.keys())
    print(f"开始同步：{targets}")
    for name in targets:
        print(f"[{name}] 抓取中...")
        rows = GROUPS[name]()
        upsert_rows(rows)
    print("全部完成。")


if __name__ == "__main__":
    main()
