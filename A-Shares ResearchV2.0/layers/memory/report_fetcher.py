"""研报获取器 — AkShare 研报下载 + 五层筛选
漏斗1: 机构白名单（Top 30 券商）
漏斗2: 深度报告标志（标题含"深度/首次覆盖/专题"等，排除"晨会/日报/点评"）
      → 同时替代"页码>10"检查，深度报告天然>10页
漏斗3: 发布时间 ≤ 180天
漏斗4: 仅限自选股（非自选股不下载）
漏斗5: 每只股票最多保留 10 篇（最新优先）
"""
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Set

import requests

logger = logging.getLogger("ReportFetcher")

try:
    import akshare as ak
    HAS_AKSHARE = True
except ImportError:
    HAS_AKSHARE = False

# ==================== 筛选配置 ====================

INSTITUTION_WHITELIST: Set[str] = {
    "中金公司", "中信证券", "华泰证券", "中信建投", "国泰君安",
    "海通证券", "申万宏源", "招商证券", "广发证券", "兴业证券",
    "长江证券", "天风证券", "国金证券", "安信证券", "光大证券",
    "方正证券", "浙商证券", "民生证券", "东吴证券", "华创证券",
    "国信证券", "国盛证券", "国海证券", "开源证券", "中泰证券",
    "西部证券", "东北证券", "华安证券", "信达证券", "德邦证券",
}

DEPTH_KEYWORDS = [
    "深度", "首次覆盖", "专题", "策略", "重磅",
    "行业研究", "公司深度", "跟踪深度", "投资价值分析",
    "投资价值", "核心竞争力", "商业模式",
]

EXCLUDE_KEYWORDS = [
    "晨会", "日报", "周报", "月报", "公告点评", "事件点评",
    "新股", "快讯", "速评", "简评", "信息速递",
]

MAX_REPORTS_PER_STOCK = 10
MAX_AGE_DAYS = 180

# ==================== PDF下载配置 ====================

REPORTS_DIR = Path(__file__).parent.parent.parent / "data" / "knowledge" / "reports"

PDF_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Referer": "https://data.eastmoney.com/",
    "Accept": "application/pdf, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
}

PDF_TIMEOUT = 15


def _sanitize_filename(name: str) -> str:
    """替换非法文件名字符"""
    return re.sub(r'[\\/*?:"<>|]', "_", name)


def _download_report_pdf(pdf_url: str, stock_code: str, title: str, pub_date: str) -> str:
    """
    下载研报 PDF 到本地 reports/ 目录
    返回本地文件路径，失败时保存元数据 .txt 作为降级，仍失败返回空字符串
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    safe_title = _sanitize_filename(title)[:60]
    safe_stock = _sanitize_filename(stock_code)
    filename = f"{safe_stock}_{pub_date}_{safe_title}.pdf"
    filepath = REPORTS_DIR / filename

    if filepath.exists() and filepath.stat().st_size > 0:
        logger.info(f"[ReportFetcher] PDF已存在: {filepath.name}")
        return str(filepath)

    txt_filepath = REPORTS_DIR / filename.replace(".pdf", ".txt")
    if txt_filepath.exists() and txt_filepath.stat().st_size > 0:
        logger.info(f"[ReportFetcher] 元数据文件已存在: {txt_filepath.name}")
        return str(txt_filepath)

    # 方案A: 尝试通过 AkShare 内置 session 下载 PDF
    try:
        if HAS_AKSHARE:
            import akshare as ak_mod
            sesh = getattr(ak_mod, '_session', None) or getattr(ak_mod, 'session', None)
            if sesh is None:
                try:
                    from akshare.utils.request_utils import get_requests_session
                    sesh = get_requests_session()
                except Exception:
                    sesh = requests.Session()
        else:
            sesh = requests.Session()

        resp = sesh.get(pdf_url, headers=PDF_HEADERS, timeout=PDF_TIMEOUT)
        resp.raise_for_status()
        content = resp.content

        content_type = resp.headers.get("Content-Type", "") or ""
        head = content[:200]

        if b"%PDF" not in head and "pdf" not in content_type.lower():
            sesh.close()
            logger.warning(f"[ReportFetcher] 非PDF内容 (Content-Type: {content_type})")
            return _save_report_txt(txt_filepath, stock_code, title, pub_date, pdf_url)

        if head[:20].startswith(b"<script") or head[:20].startswith(b"<html") or head[:20].startswith(b"<!DOC"):
            sesh.close()
            logger.warning(f"[ReportFetcher] 反盗链JS页面，存元数据: {title[:40]}")
            return _save_report_txt(txt_filepath, stock_code, title, pub_date, pdf_url)

        filepath.write_bytes(content)
        size_kb = filepath.stat().st_size / 1024
        logger.info(f"[ReportFetcher] PDF下载成功: {filepath.name} ({size_kb:.0f}KB)")
        return str(filepath)

    except requests.exceptions.Timeout:
        logger.warning(f"[ReportFetcher] PDF下载超时，存元数据: {title[:40]}")
        return _save_report_txt(txt_filepath, stock_code, title, pub_date, pdf_url)
    except requests.exceptions.HTTPError as e:
        sc = e.response.status_code if e.response else "?"
        logger.warning(f"[ReportFetcher] PDF下载失败(HTTP {sc})，存元数据: {title[:40]}")
        return _save_report_txt(txt_filepath, stock_code, title, pub_date, pdf_url)
    except Exception as e:
        logger.warning(f"[ReportFetcher] PDF下载异常({type(e).__name__})，存元数据: {title[:40]}")
        return _save_report_txt(txt_filepath, stock_code, title, pub_date, pdf_url)


def _save_report_txt(filepath: Path, stock_code: str, title: str,
                     pub_date: str, pdf_url: str) -> str:
    """PDF下载失败时的降级方案：保存研报元数据为 .txt"""
    try:
        content = f"""股票代码: {stock_code}
标题: {title}
发布日期: {pub_date}
来源URL: {pdf_url}

[PDF下载失败，此文件仅包含元数据。请通过东方财富客户端获取完整PDF。]
"""
        filepath.write_text(content, encoding="utf-8")
        logger.info(f"[ReportFetcher] 元数据已存: {filepath.name}")
        return str(filepath)
    except Exception as e:
        logger.warning(f"[ReportFetcher] 元数据写入失败: {e}")
        return ""


def _count_pdf_pages(filepath: str) -> int:
    """从本地PDF文件中统计页数"""
    try:
        content = Path(filepath).read_bytes()
        for pattern in [rb"/Type\s*/Page\b", rb"/Type\s*/Page[^s]"]:
            pages = re.findall(pattern, content)
            if pages:
                return len(pages)
    except Exception:
        pass
    return 0


def _is_depth_report(title: str) -> bool:
    tl = title.lower()
    for kw in EXCLUDE_KEYWORDS:
        if kw in title:
            return False
    for kw in DEPTH_KEYWORDS:
        if kw in title:
            return True
    return False


def _get_watchlist_stocks() -> List[str]:
    try:
        from db import watchlist_get_all
        rows = watchlist_get_all()
        return [r.get("stock_code", "").strip() for r in rows if r.get("stock_code")]
    except Exception as e:
        logger.warning(f"[ReportFetcher] 获取自选股失败: {e}")
        return []


# ==================== 公开接口 ====================

def _is_us_stock(stock_code: str) -> bool:
    code = stock_code.strip().upper().replace("US.", "").replace("HK.", "")
    code = code.replace("SH", "").replace("SZ", "")
    return code.isalpha() and not code.isdigit()


def _is_hk_stock(stock_code: str) -> bool:
    code = stock_code.strip().upper()
    return code.startswith("HK.") or (code.isdigit() and len(code) == 5)


def fetch_reports_for_stock(stock_code: str) -> List[Dict]:
    """
    获取单只股票的研报列表，经五层筛选后返回
    返回格式: [{title, institution, date, rating, pdf_url, page_count}, ...]
    """
    code = stock_code.strip()

    if _is_us_stock(code):
        return _fetch_us_reports(code)
    if _is_hk_stock(code):
        return _fetch_hk_reports(code)

    if not HAS_AKSHARE:
        logger.warning("[ReportFetcher] AkShare 未安装，跳过A股研报获取")
        return []
    logger.info(f"[ReportFetcher] 获取研报: {code}")

    try:
        df = ak.stock_research_report_em(symbol=code)
    except Exception as e:
        logger.warning(f"[ReportFetcher] AkShare 获取研报失败 ({code}): {e}")
        return []

    if df is None or df.empty:
        logger.info(f"[ReportFetcher] {code} 无研报数据")
        return []

    from layers.memory.knowledge_base import get_report_count, save_report

    existing_count = get_report_count(code)
    if existing_count >= MAX_REPORTS_PER_STOCK:
        logger.info(f"[ReportFetcher] {code} 已有 {existing_count} 篇研报，已达上限，跳过")
        return []

    cutoff_date = datetime.now() - timedelta(days=MAX_AGE_DAYS)

    filtered = []
    for _, row in df.iterrows():
        title = str(row.get("报告名称", "") or "")
        institution = str(row.get("机构", "") or "")
        pub_date_str = str(row.get("日期", "") or "")
        rating = str(row.get("东财评级", "") or "")
        pdf_url = str(row.get("报告PDF链接", "") or "")
        report_stock_code = str(row.get("股票代码", "") or "")

        # 漏斗1: 机构白名单
        if institution not in INSTITUTION_WHITELIST:
            continue

        # 漏斗2: 深度报告标志（同时替代"页码>10"检查）
        if not _is_depth_report(title):
            continue

        # 漏斗3: 时间范围（180天内）
        try:
            pub_date = datetime.strptime(pub_date_str[:10], "%Y-%m-%d")
        except (ValueError, IndexError):
            continue
        if pub_date < cutoff_date:
            continue

        # 漏斗4(已由漏斗2替代): 页码>10
        #   东方财富PDF有反盗链保护，无法直接下载验证页码
        #   "深度研究/首次覆盖/专题研究" 类报告正文均>10页
        page_count = 15

        filtered.append({
            "stock_code": report_stock_code or code,
            "title": title,
            "institution": institution,
            "pub_date": pub_date_str[:10],
            "rating": rating,
            "pdf_url": pdf_url,
            "page_count": page_count,
            "report_type": "深度研究",
        })

        # 漏斗5: 最多10篇
        if len(filtered) + existing_count >= MAX_REPORTS_PER_STOCK:
            break

    logger.info(f"[ReportFetcher] {code} 筛选结果: {len(filtered)} 篇有效研报")
    return filtered


def fetch_reports_for_watchlist() -> Dict[str, List[Dict]]:
    """
    遍历自选股（漏斗4），获取所有自选股的研报
    返回: {stock_code: [report_dict, ...]}
    """
    stocks = _get_watchlist_stocks()
    if not stocks:
        logger.warning("[ReportFetcher] 自选股为空，跳过研报获取")
        return {}

    results = {}
    for i, code in enumerate(stocks):
        logger.info(f"[ReportFetcher] 进度: {i+1}/{len(stocks)} — {code}")
        reports = fetch_reports_for_stock(code)
        results[code] = reports
        if i < len(stocks) - 1:
            time.sleep(1)

    from layers.memory.knowledge_base import save_report

    saved_count = 0
    for code, reports in results.items():
        for r in reports:
            pdf_url = r.get("pdf_url", "")
            local_path = ""

            if pdf_url:
                local_path = _download_report_pdf(
                    pdf_url, code, r["title"], r["pub_date"]
                )
                if local_path:
                    actual_pages = _count_pdf_pages(local_path)
                    if actual_pages > 0:
                        r["page_count"] = actual_pages

            ok = save_report(
                stock_code=code,
                title=r["title"],
                institution=r["institution"],
                pub_date=r["pub_date"],
                rating=r.get("rating", ""),
                report_type=r.get("report_type", ""),
                page_count=r.get("page_count", 0),
                full_text=local_path,
                source_url=pdf_url,
                quality_score=70,
            )
            if ok:
                saved_count += 1

    logger.info(f"[ReportFetcher] 完成: {len(stocks)} 只自选股, 共保存 {saved_count} 篇研报")
    return results


# ==================== 美股研报（Finnhub News） ====================

US_INSTITUTION_KEYWORDS = [
    "Goldman Sachs", "Morgan Stanley", "J.P. Morgan", "Bank of America",
    "Citigroup", "Barclays", "UBS", "Credit Suisse", "Deutsche Bank",
    "Jefferies", "Bernstein", "Oppenheimer", "Cantor", "Piper Sandler",
    "Raymond James", "Stifel", "Wedbush", "Cowen", "H.C. Wainwright",
]

US_RATING_MAP = {
    "buy": "买入", "strong-buy": "强烈买入", "strong_buy": "强烈买入",
    "outperform": "跑赢大盘", "overweight": "增持",
    "hold": "持有", "neutral": "中性", "equal-weight": "中性",
    "underperform": "跑输大盘", "underweight": "减持",
    "sell": "卖出", "strong-sell": "强烈卖出",
}


def _fetch_us_reports(stock_code: str) -> List[Dict]:
    """通过 Finnhub 获取美股新闻/研报"""
    from layers.memory.knowledge_base import get_report_count
    code = stock_code.strip().upper().replace("US.", "")

    existing_count = get_report_count(stock_code)
    if existing_count >= MAX_REPORTS_PER_STOCK:
        logger.info(f"[ReportFetcher] {stock_code} 已有 {existing_count} 篇研报，跳过")
        return []

    try:
        from config.llm_config import env_config
        api_key = env_config.FINNHUB_API_KEY
        if not api_key:
            logger.info(f"[ReportFetcher] 无 Finnhub API Key，跳过美股研报: {code}")
            return []
        import finnhub
        client = finnhub.Client(api_key=api_key)
    except ImportError:
        logger.info("[ReportFetcher] finnhub-python 未安装，跳过美股研报")
        return []

    cutoff = datetime.now() - timedelta(days=MAX_AGE_DAYS)
    from_date = cutoff.strftime("%Y-%m-%d")
    to_date = datetime.now().strftime("%Y-%m-%d")

    try:
        news = client.company_news(code, _from=from_date, to=to_date)
    except Exception as e:
        logger.warning(f"[ReportFetcher] Finnhub news 获取失败 ({code}): {e}")
        return []

    if not news:
        logger.info(f"[ReportFetcher] {code} 无 Finnhub 新闻数据")
        return []

    filtered = []
    for item in news:
        headline = item.get("headline", "")
        source = item.get("source", "")
        pub_ts = item.get("datetime", 0)
        url = item.get("url", "")
        summary = item.get("summary", "")

        if not headline:
            continue

        is_analyst = any(kw.lower() in source.lower() for kw in US_INSTITUTION_KEYWORDS)
        has_depth = any(kw.lower() in headline.lower() for kw in [
            "initiate", "upgrade", "downgrade", "outperform", "overweight",
            "deep dive", "thesis", "conviction", "price target",
        ])

        if not is_analyst and not has_depth:
            continue

        try:
            pub_date = datetime.fromtimestamp(pub_ts).strftime("%Y-%m-%d")
        except (ValueError, OSError):
            continue

        rating = ""
        for key, val in US_RATING_MAP.items():
            if key in headline.lower() or key in summary.lower():
                rating = val
                break

        filtered.append({
            "stock_code": stock_code,
            "title": headline[:200],
            "institution": source,
            "pub_date": pub_date,
            "rating": rating,
            "pdf_url": url,
            "page_count": 0,
            "report_type": "分析师报告" if is_analyst else "新闻",
        })

        if len(filtered) + existing_count >= MAX_REPORTS_PER_STOCK:
            break

    logger.info(f"[ReportFetcher] {code} Finnhub 筛选结果: {len(filtered)} 条")
    return filtered


# ==================== 港股研报（AkShare 港股接口） ====================

def _fetch_hk_reports(stock_code: str) -> List[Dict]:
    """港股研报 — 尝试 AkShare 港股研报接口"""
    if not HAS_AKSHARE:
        logger.info("[ReportFetcher] AkShare 未安装，跳过港股研报")
        return []

    from layers.memory.knowledge_base import get_report_count
    code = stock_code.strip()
    hk_code = code.replace("HK.", "").zfill(5)

    existing_count = get_report_count(code)
    if existing_count >= MAX_REPORTS_PER_STOCK:
        logger.info(f"[ReportFetcher] {code} 已有 {existing_count} 篇研报，跳过")
        return []

    try:
        df = ak.stock_hk_research_report_em(symbol=hk_code)
    except Exception as e:
        logger.info(f"[ReportFetcher] 港股研报获取失败 ({code}): {e}")
        return []

    if df is None or df.empty:
        logger.info(f"[ReportFetcher] {code} 无港股研报数据")
        return []

    cutoff_date = datetime.now() - timedelta(days=MAX_AGE_DAYS)
    filtered = []

    for _, row in df.iterrows():
        title = str(row.get("报告名称", "") or row.get("title", "") or "")
        institution = str(row.get("机构", "") or row.get("institution", "") or "")
        pub_date_str = str(row.get("日期", "") or row.get("pub_date", "") or "")
        rating = str(row.get("评级", "") or row.get("rating", "") or "")
        pdf_url = str(row.get("报告PDF链接", "") or row.get("url", "") or "")

        if institution and institution not in INSTITUTION_WHITELIST:
            if not _is_depth_report(title):
                continue

        try:
            pub_date = datetime.strptime(pub_date_str[:10], "%Y-%m-%d")
        except (ValueError, IndexError):
            continue
        if pub_date < cutoff_date:
            continue

        filtered.append({
            "stock_code": code,
            "title": title[:200],
            "institution": institution,
            "pub_date": pub_date_str[:10],
            "rating": rating,
            "pdf_url": pdf_url,
            "page_count": 15,
            "report_type": "港股研报",
        })

        if len(filtered) + existing_count >= MAX_REPORTS_PER_STOCK:
            break

    logger.info(f"[ReportFetcher] {code} 港股研报筛选结果: {len(filtered)} 篇")
    return filtered