#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
中国宏观经济数据自动抓取脚本
============================
使用 AkShare（免费、无需 API Key，聚合国家统计局/央行/海关总署等官方数据）
抓取 CPI / PPI / PMI / M0-M2 / 社融 / 进出口 / 社零 / 工业增加值 / 固定资产投资 /
房地产 / 用电量 / 财政 等指标，写入 Supabase 的 macro_indicators 表。

使用前准备：
    pip install akshare pandas supabase requests beautifulsoup4 --break-system-packages

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
import ast
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

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
                                   r.get("当月同比增长"), "%", source="国家统计局"))
    except Exception as e:
        print(f"  [警告] PPI 抓取失败: {e}")

    # ────────────────────────────────────────────────────────────
    # 以下系列在 macro_china_cpi()/macro_china_ppi() 这两个东方财富接口
    # 里都没有：
    #   - 食品CPI/非食品CPI：走新浪财经老接口 cate=price,event=1
    #     （_sync_price_sina_history），1990年至今完整历史，一次请求全拿到。
    #   - PPI生产资料/PPI生活资料：走新浪财经老接口 cate=price,event=5
    #     （_sync_price_sina_ppi_history），1980年至今完整历史。⚠️ 用"非累计"
    #     那组数据（单月同比），不是"累计"（今年以来的累计同比，口径不同）。
    #     已用统计局新闻稿的真实数字核对过完全一致。
    #   - 核心CPI同比/环比、PPI环比：国家统计局官方数据只能通过
    #     data.stats.gov.cn 查询接口获取完整历史，但该接口目前对非中国大陆
    #     出口 IP 返回 403（换路径/参数也试过，暂未找到可行的绕过方式）。改用
    #     同属国家统计局、不受此限制的新闻稿域名 www.stats.gov.cn ——月度
    #     CPI/PPI发布文章本身带这些数据（CPI 文章自带结构化 <table>，同一行里
    #     环比、同比都有；PPI 文章是模板化文字，用正则提取）。
    #   - 核心CPI同比/环比 早期历史（新闻稿翻页也覆盖不到的年份）：
    #     _sync_price_manual_core_cpi_history() 里人工整理录入，跟新闻稿数据
    #     重叠的月份以新闻稿抓到的真实数据为准（写入顺序保证了这点）。
    #
    # ⚠️ 注意：新闻稿方式默认只能拿到"当期最新一个月"（新闻稿本身是按月
    # 发布的单篇文章）。跑一次只增量拿到最新一期；要一次性回补历史月份，用
    #     python akshare_macro_sync.py --only price --price-history-pages 24
    # 加大翻页数，脚本会翻页去列表页里找更早月份的文章。
    # ────────────────────────────────────────────────────────────
    rows.extend(_sync_price_sina_history())
    rows.extend(_sync_price_sina_ppi_history())
    rows.extend(_sync_price_manual_core_cpi_history())
    rows.extend(_sync_price_nbs_extra())
    return rows


_NBS_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
           "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

NBS_LIST_URL = "https://www.stats.gov.cn/sj/zxfb/"

# 由 main() 里的 --price-history-pages 参数赋值；直接单独调用 sync_price()
# 而不经过 main() 时，默认只抓最新一期（=1）
PRICE_HISTORY_PAGES = 1


def _www_stats_session():
    s = requests.Session()
    s.headers.update({"User-Agent": _NBS_UA})
    return s


def _find_all_nbs_articles(session, list_url, title_regex, max_pages=1):
    """翻页扫描国家统计局"数据发布"列表页，收集标题匹配的文章链接（新→旧）。

    国家统计局这个列表页分页格式形如 index.html / index_2.html / index_3.html...
    """
    found = []
    seen = set()
    for page in range(1, max_pages + 1):
        page_url = list_url if page == 1 else urljoin(list_url, f"index_{page}.html")
        try:
            r = session.get(page_url, timeout=15)
            r.raise_for_status()
        except Exception as e:
            print(f"    列表页第 {page} 页拉取失败，停止翻页: {e}")
            break
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            title = a.get_text(strip=True)
            if title and re.match(title_regex, title):
                full_url = urljoin(page_url, a["href"])
                if full_url not in seen:
                    seen.add(full_url)
                    found.append((full_url, title))
    return found


def _parse_cpi_article(session, url):
    """从CPI月度发布文章的数据表格里解析出 食品/非食品/核心CPI 同比"""
    r = session.get(url, timeout=15)
    r.raise_for_status()
    r.encoding = r.apparent_encoding
    soup = BeautifulSoup(r.text, "html.parser")
    full_text = soup.get_text()

    m = re.search(r"(\d{4})年(\d{1,2})月份", full_text)
    if not m:
        raise RuntimeError(f"未能从CPI文章解析出期号（年月），URL: {url}")
    period_str = f"{m.group(1)}年{m.group(2)}月"

    # ⚠️ 食品CPI/非食品CPI 改由 _sync_price_sina_history() 提供（新浪财经
    # 老接口有 1990 年至今完整历史，比这里逐月抓新闻稿覆盖的历史长得多）。
    # 这里保留 核心CPI + 八大类同比环比（新浪那个数据源都没有，只能用新闻稿）。
    # 列顺序固定为：环比涨跌幅(cells[1]) | 同比涨跌幅(cells[2]) | 1—N月同比涨跌幅(cells[3])
    #
    # 八大类那几行，在"XX年XX月份居民消费价格主要数据"这张表里长这样（跟核心CPI
    # 是同一张表，行标签形如"一、食品烟酒及在外餐饮"到"八、其他用品及服务"）：
    #   一、食品烟酒及在外餐饮   -0.3   -0.8   -0.2
    #   二、衣着              -0.1    1.4    1.6
    #   ...
    # 用真实抓到的 2026年6月数据核对过，八大类同比/环比数字跟新华社/证券时报等
    # 转载的新闻稿原文完全一致。
    targets = {
        "其中：不包括食品和能源": [
            ("core_cpi_mom", "核心CPI 环比", 1),
            ("core_cpi_yoy", "核心CPI 同比", 2),
        ],
        "一、食品烟酒及在外餐饮": [
            ("cpi_food_tobacco_mom", "CPI 食品烟酒及在外餐饮 环比", 1),
            ("cpi_food_tobacco_yoy", "CPI 食品烟酒及在外餐饮 同比", 2),
        ],
        "二、衣着": [
            ("cpi_clothing_mom", "CPI 衣着 环比", 1),
            ("cpi_clothing_yoy", "CPI 衣着 同比", 2),
        ],
        "三、居住": [
            ("cpi_housing_mom", "CPI 居住 环比", 1),
            ("cpi_housing_yoy", "CPI 居住 同比", 2),
        ],
        "四、生活用品及服务": [
            ("cpi_household_mom", "CPI 生活用品及服务 环比", 1),
            ("cpi_household_yoy", "CPI 生活用品及服务 同比", 2),
        ],
        "五、交通通信": [
            ("cpi_transport_comm_mom", "CPI 交通通信 环比", 1),
            ("cpi_transport_comm_yoy", "CPI 交通通信 同比", 2),
        ],
        "六、教育文化娱乐": [
            ("cpi_education_mom", "CPI 教育文化娱乐 环比", 1),
            ("cpi_education_yoy", "CPI 教育文化娱乐 同比", 2),
        ],
        "七、医疗保健": [
            ("cpi_healthcare_mom", "CPI 医疗保健 环比", 1),
            ("cpi_healthcare_yoy", "CPI 医疗保健 同比", 2),
        ],
        "八、其他用品及服务": [
            ("cpi_other_mom", "CPI 其他用品及服务 环比", 1),
            ("cpi_other_yoy", "CPI 其他用品及服务 同比", 2),
        ],
    }
    results = []
    matched_labels = set()
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
            if not cells:
                continue
            label = cells[0]
            if label in targets and len(cells) >= 3:
                for series_code, series_name, col_idx in targets[label]:
                    try:
                        val = float(cells[col_idx].replace("−", "-"))
                    except (ValueError, IndexError):
                        continue
                    results.append((series_code, series_name, val))
                matched_labels.add(label)

    missing = set(targets) - matched_labels
    if missing:
        print(f"  [警告] CPI文章表格里未匹配到以下行：{missing}（可能是统计局调整了"
              f"分类命名，需要人工核对文章 {url} 里的表格行标签并更新脚本里的 targets 字典）")
    return period_str, results


def _parse_ppi_article(session, url):
    """从PPI月度发布文章解析：
    1) 模板化文字段落，用正则提取头条环比（原有逻辑，用于 ppi_mom）
    2) "XX年XX月工业生产者价格主要数据"表格，提取生产资料/生活资料细分
       （见下方 targets，都是"一、工业生产者出厂价格"这个小节里的行）

    ⚠️ 生产资料/生活资料同比 改由 _sync_price_sina_ppi_history() 提供
    （新浪财经历史更全、1980年至今，且已用这里的新闻稿数字核对过完全一致），
    这里不重复抓这两项的同比，只补新浪没有的：这两项的环比，以及
    采掘/原材料/加工（生产资料内部细分）、食品/衣着/一般日用品/耐用消费品
    （生活资料内部细分）的同比+环比——新浪那条线完全没有这几项。
    """
    r = session.get(url, timeout=15)
    r.raise_for_status()
    r.encoding = r.apparent_encoding
    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text()

    m = re.search(r"(\d{4})年(\d{1,2})月份", text)
    if not m:
        raise RuntimeError(f"未能从PPI文章解析出期号（年月），URL: {url}")
    period_str = f"{m.group(1)}年{m.group(2)}月"

    results = []

    # 头条：全国工业生产者出厂价格同比X%，环比Y%（用于 ppi_mom）
    m_head = re.search(
        r"工业生产者出厂价格同比(?:上涨|下降)[\d.]+%[，,]\s*环比(上涨|下降|持平)([\d.]*)%", text)
    if m_head:
        sign = -1 if m_head.group(1) == "下降" else 1
        val = float(m_head.group(2)) if m_head.group(2) else 0.0
        results.append(("ppi_mom", "PPI 环比", sign * val))
    else:
        print(f"  [警告] 未能从PPI文章解析出头条环比数据，URL: {url}")

    # "XX年XX月工业生产者价格主要数据"表格：生产资料/生活资料细分
    # 列顺序固定为：环比涨跌幅(cells[1]) | 同比涨跌幅(cells[2]) | 1—N月同比涨跌幅(cells[3])
    # 表格同一张里后面还有"二、工业生产者购进价格""三、主要行业出厂价格"两个小节，
    # 但那两个小节里的行标签（"燃料、动力类""煤炭开采和洗选业"等）跟下面这些
    # 目标行标签不会重名，不需要额外做小节边界判断。
    table_targets = {
        "生产资料": [("ppi_means_prod_mom", "PPI 生产资料 环比", 1)],  # 同比已由新浪提供，这里只补环比
        "采掘": [
            ("ppi_mining_mom", "PPI 生产资料-采掘 环比", 1),
            ("ppi_mining_yoy", "PPI 生产资料-采掘 同比", 2),
        ],
        "原材料": [
            ("ppi_raw_materials_mom", "PPI 生产资料-原材料 环比", 1),
            ("ppi_raw_materials_yoy", "PPI 生产资料-原材料 同比", 2),
        ],
        "加工": [
            ("ppi_processing_mom", "PPI 生产资料-加工 环比", 1),
            ("ppi_processing_yoy", "PPI 生产资料-加工 同比", 2),
        ],
        "生活资料": [("ppi_living_mom", "PPI 生活资料 环比", 1)],  # 同比已由新浪提供，这里只补环比
        "食品": [
            ("ppi_living_food_mom", "PPI 生活资料-食品 环比", 1),
            ("ppi_living_food_yoy", "PPI 生活资料-食品 同比", 2),
        ],
        "衣着": [
            ("ppi_living_clothing_mom", "PPI 生活资料-衣着 环比", 1),
            ("ppi_living_clothing_yoy", "PPI 生活资料-衣着 同比", 2),
        ],
        "一般日用品": [
            ("ppi_living_daily_mom", "PPI 生活资料-一般日用品 环比", 1),
            ("ppi_living_daily_yoy", "PPI 生活资料-一般日用品 同比", 2),
        ],
        "耐用消费品": [
            ("ppi_living_durable_mom", "PPI 生活资料-耐用消费品 环比", 1),
            ("ppi_living_durable_yoy", "PPI 生活资料-耐用消费品 同比", 2),
        ],
    }
    matched_labels = set()
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
            if not cells:
                continue
            label = cells[0]
            if label in table_targets and len(cells) >= 3:
                for series_code, series_name, col_idx in table_targets[label]:
                    try:
                        val = float(cells[col_idx].replace("−", "-"))
                    except (ValueError, IndexError):
                        continue
                    results.append((series_code, series_name, val))
                matched_labels.add(label)

    missing = set(table_targets) - matched_labels
    if missing:
        print(f"  [警告] PPI文章表格里未匹配到以下行：{missing}（可能是统计局调整了"
              f"分类命名，需要人工核对文章 {url} 里的表格行标签并更新脚本里的 table_targets 字典）")

    return period_str, results


def _js_literal_eval(js_text):
    """把新浪接口返回的、形似 Python 字面量的 JS 片段安全解析成 Python 对象。

    这段文本本质是 JS 对象字面量，绝大多数情况下跟 Python 字面量语法一致
    （字符串、数字、列表、字典），可以直接用 ast.literal_eval。但个别字段
    缺失时新浪会填 JS 的 null（偶尔也可能出现 true/false），这几个词
    Python 不认识（Python 对应写法是 None/True/False，且首字母大写），
    直接丢给 ast.literal_eval 会报 "malformed node or string" 错误。
    这里用 \\b 词边界替换，只替换独立出现的 null/true/false，不会误伤
    中文字符串或其他包含这几个字母的内容。
    """
    js_text = re.sub(r"\bnull\b", "None", js_text)
    js_text = re.sub(r"\btrue\b", "True", js_text)
    js_text = re.sub(r"\bfalse\b", "False", js_text)
    return ast.literal_eval(js_text)


_SINA_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# 新浪财经"居民消费项目价格指数"里，分类名 -> (我们的系列代码, 系列中文名)
# 数值本身是"上年同月=100"的指数，同比% = 指数 - 100
_SINA_CPI_CATEGORY_MAP = {
    "食品类": ("cpi_food_yoy", "食品CPI 同比"),
    "非食品": ("cpi_nonfood_yoy", "非食品CPI 同比"),
}


def _sina_get_with_retry(session, url, params, timeout=30, max_retries=3):
    """带重试的 GET 请求。

    新浪这两个历史接口一次要拉全部历史（8000-9000行），数据量偏大，
    偶尔会 Read timed out（纯网络抖动，不是接口坏了，重试一般就好）。
    这里做最多 3 次尝试，每次间隔递增（2s / 4s / 6s），并把 timeout
    从原来的 20s 提到 30s 留更多余量。最后一次尝试如果还失败，
    把异常原样往上抛，由调用方的 try/except 捕获并打印警告
    （行为和之前一致：这个类别的历史数据这次就跳过，不影响其他类别）。
    """
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_exc = e
            if attempt < max_retries:
                wait = attempt * 2
                print(f"  [提示] 请求超时/连接失败（第{attempt}次），{wait}秒后重试...")
                time.sleep(wait)
    raise last_exc


def _sina_fetch_price_history(cate="price"):
    """请求新浪财经宏观数据中心的老接口，一次性拿回某个价格类别的全部历史
    （目前用于 cate='price' 即"居民消费项目价格指数"，1990年至今，8000+行）。

    返回 list of [period_str('2026.6'), category_name, value_str]
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": _SINA_UA,
        "Referer": "https://finance.sina.com.cn/",
    })
    url = "https://quotes.sina.cn/mac/api/jsonp_v3.php/x/MacPage_Service.get_pagedata"
    params = {
        "cate": cate,
        "event": "1",
        "from": "0",
        "num": "9000",  # 覆盖全部历史（实测该数据集共 8823 行）
        "condition": "",
        "_": str(int(time.time() * 1000)),
    }
    r = _sina_get_with_retry(session, url, params)
    text = r.text
    # 返回内容形如: /*<script>...</script>*/\ncallback(({config:{...},count:"8823",data:[...]}));
    # 不是严格 JSON（key 没加引号），这里只精确摘出 data:[...] 这一段再用 ast.literal_eval 解析
    # （data 数组本身是双引号字符串的合法 Python/JSON 字面量，可以安全解析，不涉及任意代码执行）
    # 用 count:"NNNN",data: 这个组合精确锚定顶层 data 数组，不能只用 data:
    # 去匹配——config.querylist 里也有同名的 data 字段，贪婪匹配会从那里
    # 开始截取导致 ast.literal_eval 解析报语法错误（已用真实响应验证过这个坑）
    m = re.search(r'count\s*:\s*"\d+"\s*,\s*data\s*:\s*(\[.*\])\s*\}\)\)\s*;?\s*$', text, re.S)
    if not m:
        raise RuntimeError("未能在新浪返回内容中定位到 data 数组，接口格式可能已变化")
    return _js_literal_eval(m.group(1))


def _sync_price_sina_history():
    """食品CPI/非食品CPI 走新浪财经老接口，一次性拿 1990年至今完整历史。"""
    rows = []
    try:
        raw_rows = _sina_fetch_price_history(cate="price")
    except Exception as e:
        print(f"  [警告] 新浪财经CPI历史数据 抓取失败: {e}")
        return rows

    skipped_zero = 0
    for period_str, cat_name, value_str in raw_rows:
        if cat_name not in _SINA_CPI_CATEGORY_MAP:
            continue
        series_code, series_name = _SINA_CPI_CATEGORY_MAP[cat_name]
        try:
            value = float(value_str)
        except (TypeError, ValueError):
            continue
        # 个别期数（如 2013.12 的"非食品"）新浪那边录成了 0.00，明显是数据源
        # 本身的录入错误（CPI 指数不可能是 0），过滤掉，不要写脏数据进库
        if value <= 0:
            skipped_zero += 1
            continue
        try:
            y, mo = period_str.split(".")
            period_label = f"{int(y):04d}年{int(mo):02d}月"
        except ValueError:
            continue
        yoy_pct = value - 100
        rows.append(build_row("price", series_code, series_name, period_label,
                               yoy_pct, "%", source="新浪财经/国家统计局"))
    if skipped_zero:
        print(f"  [提示] 新浪CPI历史数据里有 {skipped_zero} 条明显异常的 0 值已被过滤")
    return rows


_SINA_PPI_CATEGORY_MAP = {
    "生产资料": ("ppi_means_prod_yoy", "PPI生产资料 同比"),
    "生活资料": ("ppi_living_yoy", "PPI生活资料 同比"),
}


def _sina_fetch_ppi_history(cate="price"):
    """请求新浪财经"工业品出厂价格(月度)"数据（event=5），一次性拿回全部历史
    （1980年至今）。跟 CPI 那张表结构不一样：这里的 data 字段本身是一个
    带'累计'/'非累计'两个 key 的字典，且数值已经是同比/环比百分比本身
    （不是"上年同月=100"的指数，不用再 -100）。

    返回 dict: {'累计': [[period, cat_name, yoy_str, index_str], ...],
                '非累计': [...]}
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": _SINA_UA,
        "Referer": "https://finance.sina.com.cn/",
    })
    url = "https://quotes.sina.cn/mac/api/jsonp_v3.php/x/MacPage_Service.get_pagedata"
    params = {
        "cate": cate,
        "event": "5",  # 5 = 工业品出厂价格(月度)
        "from": "0",
        "num": "9000",  # 覆盖全部历史（该数据集从1980年至今）
        "condition": "",
        "_": str(int(time.time() * 1000)),
    }
    r = _sina_get_with_retry(session, url, params)
    text = r.text
    # 这里 data 是字典 {...} 不是数组 [...]，正则相应调整
    m = re.search(r'count\s*:\s*"\d+"\s*,\s*data\s*:\s*(\{.*\})\s*\}\)\)\s*;?\s*$', text, re.S)
    if not m:
        raise RuntimeError("未能在新浪PPI返回内容中定位到 data 字典，接口格式可能已变化")
    return _js_literal_eval(m.group(1))


def _sync_price_sina_ppi_history():
    """PPI生产资料/生活资料 同比 走新浪财经老接口，一次性拿 1980年至今完整历史。

    ⚠️ 用"非累计"这组数据（单月同比），不要用"累计"（今年以来的累计同比，
    是完全不同的统计口径）——已用国家统计局新闻稿的真实数据核对过
    （2026年6月：生产资料 5.5%、生活资料 -0.9%，两边完全吻合）。
    """
    rows = []
    try:
        data_dict = _sina_fetch_ppi_history(cate="price")
    except Exception as e:
        print(f"  [警告] 新浪财经PPI历史数据 抓取失败: {e}")
        return rows

    non_accum_rows = data_dict.get("非累计")
    if non_accum_rows is None:
        print("  [警告] 新浪PPI返回内容里没有'非累计'这个 key，接口结构可能变了")
        return rows

    for period_str, cat_name, yoy_str, _index_str in non_accum_rows:
        if cat_name not in _SINA_PPI_CATEGORY_MAP:
            continue
        series_code, series_name = _SINA_PPI_CATEGORY_MAP[cat_name]
        try:
            yoy_pct = float(yoy_str)
        except (TypeError, ValueError):
            continue
        try:
            y, mo = period_str.split(".")
            period_label = f"{int(y):04d}年{int(mo):02d}月"
        except ValueError:
            continue
        rows.append(build_row("price", series_code, series_name, period_label,
                               yoy_pct, "%", source="新浪财经/国家统计局"))
    return rows


# ────────────────────────────────────────────────────────────
# 核心CPI 同比/环比 —— 早期历史人工补录
# ────────────────────────────────────────────────────────────
# 国家统计局官方查询接口 data.stats.gov.cn 对非中国大陆出口 IP 返回 403，
# 脚本目前只能通过新闻稿（www.stats.gov.cn 月度发布文章，见 _parse_cpi_article）
# 逐月获取核心CPI 同比/环比。新闻稿是按月发布的单篇文章，翻页也只能回补
# 有限的历史月份，覆盖不到更早的年份（如2012年及以前的同比、2018年以前的环比）。
#
# 下面两段是网上找到并核对过的历史序列，人工整理录入，不是本脚本自动抓取的。
# 后续如果新闻稿抓取到了同一个 period 的真实数据（sync_price() 里
# _sync_price_nbs_extra() 在本函数之后调用），upsert_rows 按 period 去重时
# 会保留列表里更晚出现的那条，也就是新闻稿数据会覆盖这里的人工数据——
# 所以两边即使月份有重叠也不用担心冲突，新闻稿数据始终优先。
#
# 如果以后想更新/扩充这两段数据，直接编辑这两个字符串就行，格式是
# csv 两列 date,value（每行一个月，date 用当月1号，value 是百分比数字）。
# ────────────────────────────────────────────────────────────

_CORE_CPI_YOY_MANUAL = """
date,value
2008-01-01,1.5
2008-02-01,1.6
2008-03-01,1.4
2008-04-01,1.4
2008-05-01,1.5
2008-06-01,1.7
2008-07-01,1.9
2008-08-01,1.9
2008-09-01,1.7
2008-10-01,1.6
2008-11-01,1.4
2008-12-01,1.3
2009-01-01,1.1
2009-02-01,0.6
2009-03-01,-0.1
2009-04-01,-0.3
2009-05-01,-0.3
2009-06-01,-0.2
2009-07-01,-0.3
2009-08-01,-0.1
2009-09-01,0.1
2009-10-01,0.2
2009-11-01,0.5
2009-12-01,1.1
2010-01-01,1.1
2010-02-01,1.1
2010-03-01,1.2
2010-04-01,1.2
2010-05-01,1.3
2010-06-01,1.4
2010-07-01,1.5
2010-08-01,1.5
2010-09-01,1.6
2010-10-01,1.6
2010-11-01,1.9
2010-12-01,2.1
2011-01-01,2.4
2011-02-01,2.3
2011-03-01,2.3
2011-04-01,2.2
2011-05-01,2.2
2011-06-01,2.3
2011-07-01,2.4
2011-08-01,2.3
2011-09-01,2.2
2011-10-01,2.1
2011-11-01,1.9
2011-12-01,1.9
2012-01-01,2.0
2012-02-01,1.6
2012-03-01,1.8
2012-04-01,1.7
2012-05-01,1.6
2012-06-01,1.6
2012-07-01,1.5
2012-08-01,1.7
2012-09-01,1.7
2012-10-01,1.8
2012-11-01,1.9
2012-12-01,1.9
"""

_CORE_CPI_MOM_MANUAL = """
date,value
2018-01-01,0.2
2018-02-01,0.6
2018-03-01,-0.2
2018-04-01,0.1
2018-05-01,0.1
2018-06-01,0.1
2018-07-01,0.2
2018-08-01,0.2
2018-09-01,0.1
2018-10-01,0.1
2018-11-01,0.0
2018-12-01,0.1
2019-01-01,0.2
2019-02-01,0.3
2019-03-01,-0.1
2019-04-01,0.1
2019-05-01,0.1
2019-06-01,0.1
2019-07-01,0.2
2019-08-01,0.1
2019-09-01,0.1
2019-10-01,0.1
2019-11-01,-0.1
2019-12-01,-0.1
2020-01-01,0.4
2020-02-01,-0.1
2020-03-01,-0.3
2020-04-01,-0.2
2020-05-01,0.0
2020-06-01,-0.1
2020-07-01,0.0
2020-08-01,0.1
2020-09-01,0.0
2020-10-01,0.1
2020-11-01,-0.1
2020-12-01,0.1
2021-01-01,-0.2
2021-02-01,0.2
2021-03-01,-0.2
2021-04-01,0.3
2021-05-01,0.1
2021-06-01,0.1
2021-07-01,0.3
2021-08-01,0.0
2021-09-01,0.2
2021-10-01,0.1
2021-11-01,-0.2
2021-12-01,0.0
2022-01-01,0.1
2022-02-01,0.1
2022-03-01,-0.1
2022-04-01,0.1
2022-05-01,-0.1
2022-06-01,0.1
2022-07-01,0.3
2022-08-01,0.0
2022-09-01,0.0
2022-10-01,0.1
2022-11-01,-0.2
2022-12-01,0.1
2023-01-01,0.4
2023-02-01,-0.2
2023-03-01,0.0
2023-04-01,0.1
2023-05-01,-0.2
2023-06-01,0.1
2023-07-01,0.5
2023-08-01,0.0
2023-09-01,0.1
2023-10-01,0.0
2023-11-01,-0.3
2023-12-01,0.1
2024-01-01,0.3
2024-02-01,0.5
2024-03-01,-0.6
2024-04-01,0.2
2024-05-01,-0.2
2024-06-01,-0.1
2024-07-01,0.4
2024-08-01,-0.1
2024-09-01,-0.1
2024-10-01,0.0
2024-11-01,-0.1
2024-12-01,0.1
2025-01-01,0.1
2025-02-01,0.5
2025-03-01,-0.3
2025-04-01,0.1
2025-05-01,-0.1
2025-06-01,0.1
2025-07-01,0.2
2025-08-01,0.0
2025-09-01,0.1
2025-10-01,0.0
2025-11-01,0.5
2025-12-01,0.1
2026-01-01,0.2
2026-02-01,0.6
2026-03-01,-0.3
2026-04-01,0.1
2026-05-01,-0.1
"""


def _parse_manual_csv_block(text, series_code, series_name):
    """把上面那种 date,value 两列的 csv 文本块解析成 rows"""
    rows = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line or line.lower().startswith("date"):
            continue
        date_str, value_str = line.split(",")
        rows.append(build_row("price", series_code, series_name, date_str,
                               float(value_str), "%", source="网络公开历史数据（人工整理）"))
    return rows


def _sync_price_manual_core_cpi_history():
    """核心CPI 同比/环比 的早期历史人工补录，详见上方大段注释。"""
    rows = []
    rows.extend(_parse_manual_csv_block(_CORE_CPI_YOY_MANUAL, "core_cpi_yoy", "核心CPI 同比"))
    rows.extend(_parse_manual_csv_block(_CORE_CPI_MOM_MANUAL, "core_cpi_mom", "核心CPI 环比"))
    return rows


def _sync_price_nbs_extra():
    """通过国家统计局"数据发布"新闻稿页面（www.stats.gov.cn）补充
    CPI/PPI 的细分系列，见上方 sync_price() 里的说明。
    """
    rows = []
    session = _www_stats_session()

    try:
        cpi_articles = _find_all_nbs_articles(
            session, NBS_LIST_URL, r"^\d{4}年\d{1,2}月份居民消费价格",
            max_pages=PRICE_HISTORY_PAGES)
        if not cpi_articles:
            print("  [警告] 未在数据发布列表页找到CPI新闻稿链接")
        for url, title in cpi_articles:
            try:
                period_str, results = _parse_cpi_article(session, url)
                for series_code, series_name, value in results:
                    rows.append(build_row("price", series_code, series_name, period_str,
                                           value, "%", source="国家统计局"))
            except Exception as e:
                print(f"  [警告] 解析CPI文章失败（{title}）: {e}")
    except Exception as e:
        print(f"  [警告] CPI 细分（核心/食品/非食品）抓取失败: {e}")

    try:
        ppi_articles = _find_all_nbs_articles(
            session, NBS_LIST_URL, r"^\d{4}年\d{1,2}月份工业生产者出厂价格",
            max_pages=PRICE_HISTORY_PAGES)
        if not ppi_articles:
            print("  [警告] 未在数据发布列表页找到PPI新闻稿链接")
        for url, title in ppi_articles:
            try:
                period_str, results = _parse_ppi_article(session, url)
                for series_code, series_name, value in results:
                    rows.append(build_row("price", series_code, series_name, period_str,
                                           value, "%", source="国家统计局"))
            except Exception as e:
                print(f"  [警告] 解析PPI文章失败（{title}）: {e}")
    except Exception as e:
        print(f"  [警告] PPI环比/生产资料/生活资料 抓取失败: {e}")

    return rows
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
        df = fetch_with_retry(ak.macro_china_cx_services_pmi_yearly)  # 财新服务业PMI（金十数据，作为主数据源）
        for _, r in df.iterrows():
            if pd.isna(r.get("今值")):
                continue
            rows.append(build_row("pmi", "caixin_pmi_svc", "财新服务业PMI", r["日期"],
                                   r.get("今值"), "指数", source="财新/Markit"))
    except Exception as e:
        print(f"  [警告] 财新服务业PMI（金十数据源）抓取失败: {e}")

    try:
        # 财新服务业PMI 备用数据源（财新智库直连），当上面金十数据源缺数据时用这个补充
        df = fetch_with_retry(ak.index_pmi_ser_cx)  # 字段: 日期/服务业PMI/变化值
        for _, r in df.iterrows():
            if pd.isna(r.get("服务业PMI")):
                continue
            rows.append(build_row("pmi", "caixin_pmi_svc", "财新服务业PMI", r["日期"],
                                   r.get("服务业PMI"), "指数", source="财新智库"))
    except Exception as e:
        print(f"  [警告] 财新服务业PMI（备用数据源）抓取失败: {e}")

    try:
        df = fetch_with_retry(ak.index_pmi_com_cx)  # 财新中国综合PMI产出指数，字段: 日期/综合PMI/变化值
        for _, r in df.iterrows():
            if pd.isna(r.get("综合PMI")):
                continue
            rows.append(build_row("pmi", "pmi_composite", "综合PMI产出指数", r["日期"],
                                   r.get("综合PMI"), "指数", source="财新智库"))
    except Exception as e:
        print(f"  [警告] 综合PMI产出指数 抓取失败: {e}")

    return rows


def sync_money():
    rows = []
    try:
        df = fetch_with_retry(ak.macro_china_money_supply)  # M0/M1/M2
        for _, r in df.iterrows():
            period = r["月份"]
            # ⚠️ 这里之前有个 bug：m0_yoy 错误地读了 M2 那一列（复制粘贴漏改），
            # 导致写进数据库的 m0_yoy 实际上一直是 M2 同比的数值。这次一并修正。
            rows.append(build_row("money", "m0_value", "M0(流通中的现金) 数量", period,
                                   r.get("流通中的现金(M0)-数量(亿元)"), "亿元", source="央行"))
            rows.append(build_row("money", "m0_yoy", "M0 同比", period,
                                   r.get("流通中的现金(M0)-同比增长"), "%", source="央行"))
            rows.append(build_row("money", "m0_mom", "M0 环比", period,
                                   r.get("流通中的现金(M0)-环比增长"), "%", source="央行"))
            rows.append(build_row("money", "m1_value", "M1 数量", period,
                                   r.get("货币(M1)-数量(亿元)"), "亿元", source="央行"))
            rows.append(build_row("money", "m1_yoy", "M1 同比", period,
                                   r.get("货币(M1)-同比增长"), "%", source="央行"))
            rows.append(build_row("money", "m1_mom", "M1 环比", period,
                                   r.get("货币(M1)-环比增长"), "%", source="央行"))
            rows.append(build_row("money", "m2_value", "M2(货币和准货币) 数量", period,
                                   r.get("货币和准货币(M2)-数量(亿元)"), "亿元", source="央行"))
            rows.append(build_row("money", "m2_yoy", "M2 同比", period,
                                   r.get("货币和准货币(M2)-同比增长"), "%", source="央行"))
            rows.append(build_row("money", "m2_mom", "M2 环比", period,
                                   r.get("货币和准货币(M2)-环比增长"), "%", source="央行"))
    except Exception as e:
        print(f"  [警告] M0/M1/M2 抓取失败: {e}")

    try:
        df = fetch_with_retry(ak.macro_china_shrzgm)  # 社会融资规模增量（含6项细分）
        for _, r in df.iterrows():
            period = r["月份"]
            rows.append(build_row("money", "shrzgm_increment", "社会融资规模增量", period,
                                   r.get("社会融资规模增量"), "亿元", source="央行"))
            rows.append(build_row("money", "shrzgm_rmb_loan", "社融-人民币贷款", period,
                                   r.get("其中-人民币贷款"), "亿元", source="央行"))
            rows.append(build_row("money", "shrzgm_entrusted_fx_loan", "社融-委托贷款外币贷款", period,
                                   r.get("其中-委托贷款外币贷款"), "亿元", source="央行"))
            rows.append(build_row("money", "shrzgm_entrusted_loan", "社融-委托贷款", period,
                                   r.get("其中-委托贷款"), "亿元", source="央行"))
            rows.append(build_row("money", "shrzgm_trust_loan", "社融-信托贷款", period,
                                   r.get("其中-信托贷款"), "亿元", source="央行"))
            rows.append(build_row("money", "shrzgm_bank_acceptance", "社融-未贴现银行承兑汇票", period,
                                   r.get("其中-未贴现银行承兑汇票"), "亿元", source="央行"))
            rows.append(build_row("money", "shrzgm_corp_bond", "社融-企业债券", period,
                                   r.get("其中-企业债券"), "亿元", source="央行"))
            rows.append(build_row("money", "shrzgm_equity_financing", "社融-非金融企业境内股票融资", period,
                                   r.get("其中-非金融企业境内股票融资"), "亿元", source="央行"))
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
            period = r["月份"]
            rows.append(build_row("retail", "retail_value", "社零 当月值", period,
                                   r.get("当月"), "亿元", source="国家统计局"))
            rows.append(build_row("retail", "retail_yoy", "社零 同比", period,
                                   r.get("同比增长"), "%", source="国家统计局"))
            rows.append(build_row("retail", "retail_mom", "社零 环比", period,
                                   r.get("环比增长"), "%", source="国家统计局"))
            rows.append(build_row("retail", "retail_accumulate", "社零 累计值", period,
                                   r.get("累计"), "亿元", source="国家统计局"))
            rows.append(build_row("retail", "retail_accumulate_yoy", "社零 累计同比", period,
                                   r.get("累计-同比增长"), "%", source="国家统计局"))
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
            period = r["月份"]
            rows.append(build_row("fiscal", "fiscal_revenue_value", "财政收入 当月值", period,
                                   r.get("当月"), "亿元", source="财政部"))
            rows.append(build_row("fiscal", "fiscal_revenue_yoy", "财政收入 当月同比", period,
                                   r.get("当月-同比增长"), "%", source="财政部"))
            rows.append(build_row("fiscal", "fiscal_revenue_mom", "财政收入 当月环比", period,
                                   r.get("当月-环比增长"), "%", source="财政部"))
            rows.append(build_row("fiscal", "fiscal_revenue_accumulate", "财政收入 累计值", period,
                                   r.get("累计"), "亿元", source="财政部"))
            rows.append(build_row("fiscal", "fiscal_revenue_accumulate_yoy", "财政收入 累计同比", period,
                                   r.get("累计-同比增长"), "%", source="财政部"))
    except Exception as e:
        print(f"  [警告] 财政收入 抓取失败: {e}")
    return rows


def sync_investment():
    """固定资产投资（城镇），akshare: macro_china_gdzctz。当月/同比/环比/自年初累计。"""
    rows = []
    try:
        df = fetch_with_retry(ak.macro_china_gdzctz)
        for _, r in df.iterrows():
            period = r["月份"]
            rows.append(build_row("investment", "fai_value", "固定资产投资 当月值", period,
                                   r.get("当月"), "亿元", source="国家统计局"))
            rows.append(build_row("investment", "fai_yoy", "固定资产投资 同比", period,
                                   r.get("同比增长"), "%", source="国家统计局"))
            rows.append(build_row("investment", "fai_mom", "固定资产投资 环比", period,
                                   r.get("环比增长"), "%", source="国家统计局"))
            rows.append(build_row("investment", "fai_ytd", "固定资产投资 自年初累计", period,
                                   r.get("自年初累计"), "亿元", source="国家统计局"))
    except Exception as e:
        print(f"  [警告] 固定资产投资 抓取失败: {e}")
    return rows


def sync_credit():
    """新增信贷（新增人民币贷款），akshare: macro_china_new_financial_credit。"""
    rows = []
    try:
        df = fetch_with_retry(ak.macro_china_new_financial_credit)
        for _, r in df.iterrows():
            period = r["月份"]
            rows.append(build_row("credit", "new_rmb_loan_value", "新增人民币贷款 当月值", period,
                                   r.get("当月"), "亿元", source="央行"))
            rows.append(build_row("credit", "new_rmb_loan_yoy", "新增人民币贷款 同比", period,
                                   r.get("当月-同比增长"), "%", source="央行"))
            rows.append(build_row("credit", "new_rmb_loan_mom", "新增人民币贷款 环比", period,
                                   r.get("当月-环比增长"), "%", source="央行"))
            rows.append(build_row("credit", "new_rmb_loan_accumulate", "新增人民币贷款 累计值", period,
                                   r.get("累计"), "亿元", source="央行"))
            rows.append(build_row("credit", "new_rmb_loan_accumulate_yoy", "新增人民币贷款 累计同比", period,
                                   r.get("累计-同比增长"), "%", source="央行"))
    except Exception as e:
        print(f"  [警告] 新增信贷 抓取失败: {e}")
    return rows


def _tax_quarter_to_period(quarter_str):
    """把税收收入接口里"YYYY年第N季度"/"YYYY年第N-M季度"这种累计季度格式，
    转成 (period, period_label)。

    ⚠️ 注意：这个字段本身就是"累计"口径（比如"2022年第1-3季度"指的是
    前三季度累计税收收入，不是单独第三季度），跟当月/当季数据的含义不一样，
    每年年初（第1季度）会重新从0开始累计。这里用累计区间里最后一个季度的
    季末月份作为 period（比如"第1-3季度"取9月30日所在月，即当年09月）。
    """
    m = re.match(r"(\d{4})年第(\d)(?:-(\d))?季度", quarter_str.strip())
    if not m:
        raise ValueError(f"无法解析税收季度格式: {quarter_str}")
    year = int(m.group(1))
    end_q = int(m.group(3)) if m.group(3) else int(m.group(2))
    end_month = end_q * 3
    return f"{year:04d}-{end_month:02d}-01", f"{year:04d}-{end_month:02d}"


def sync_tax():
    """全国税收收入（季度，累计口径），akshare: macro_china_national_tax_receipts。"""
    rows = []
    try:
        df = fetch_with_retry(ak.macro_china_national_tax_receipts)
        for _, r in df.iterrows():
            try:
                period, label = _tax_quarter_to_period(r["季度"])
            except ValueError as e:
                print(f"  [警告] {e}，跳过这一行")
                continue
            for series_code, series_name, col, unit in [
                ("tax_revenue_accumulate", "全国税收收入 累计值（当年）", "税收收入合计", "亿元"),
                ("tax_revenue_yoy", "全国税收收入 较上年同期", "较上年同期", "%"),
                ("tax_revenue_mom", "全国税收收入 季度环比", "季度环比", "%"),
            ]:
                val = r.get(col)
                if pd.isna(val):
                    continue
                rows.append({
                    "category": "tax",
                    "series_code": series_code,
                    "series_name": series_name,
                    "period": period,
                    "period_label": label,
                    "value": float(val),
                    "unit": unit,
                    "yoy": None,
                    "mom": None,
                    "source": "财政部/税务总局",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
    except Exception as e:
        print(f"  [警告] 全国税收收入 抓取失败: {e}")
    return rows


def sync_realestate():
    """国房景气指数，akshare: macro_china_real_estate。"""
    rows = []
    try:
        df = fetch_with_retry(ak.macro_china_real_estate)
        for _, r in df.iterrows():
            period = r["日期"]
            rows.append(build_row("realestate", "real_estate_index", "国房景气指数", period,
                                   r.get("最新值"), "指数", source="国家统计局"))
            rows.append(build_row("realestate", "real_estate_index_mom", "国房景气指数 涨跌幅（环比）", period,
                                   r.get("涨跌幅"), "%", source="国家统计局"))
    except Exception as e:
        print(f"  [警告] 国房景气指数 抓取失败: {e}")
    return rows


GROUPS = {
    "price": sync_price,
    "pmi": sync_pmi,
    "money": sync_money,
    "trade": sync_trade,
    "retail": sync_retail,
    "electricity": sync_electricity,
    "fiscal": sync_fiscal,
    "investment": sync_investment,      # 固定资产投资（城镇）
    "credit": sync_credit,              # 新增人民币贷款
    "tax": sync_tax,                    # 全国税收收入（季度，累计口径）
    "realestate": sync_realestate,      # 国房景气指数
    # auto（汽车产销）、industry 中的工业企业利润 目前建议用 macro_entry.html 手动录入。
}


def main():
    parser = argparse.ArgumentParser(description="中国宏观经济数据同步脚本")
    parser.add_argument("--only", nargs="*", choices=list(GROUPS.keys()),
                         help="只同步指定分组，默认同步全部")
    parser.add_argument("--price-history-pages", type=int, default=1,
                         help="价格指数细分系列（核心/食品/非食品CPI、PPI生产资料/生活资料）"
                              "通过国家统计局新闻稿页面抓取，默认只抓最新一期（=1）。"
                              "首次运行想一次性回补历史月份时可以调大，例如 --price-history-pages 24 "
                              "大致覆盖 2 年（国家统计局'数据发布'列表页每页条数会随发布类型混排变化，"
                              "实际覆盖月数以运行时打印的抓取到的文章数量为准）。")
    args = parser.parse_args()

    global PRICE_HISTORY_PAGES
    PRICE_HISTORY_PAGES = args.price_history_pages

    targets = args.only if args.only else list(GROUPS.keys())
    print(f"开始同步：{targets}")
    for name in targets:
        print(f"[{name}] 抓取中...")
        rows = GROUPS[name]()
        upsert_rows(rows)
    print("全部完成。")


if __name__ == "__main__":
    main()
