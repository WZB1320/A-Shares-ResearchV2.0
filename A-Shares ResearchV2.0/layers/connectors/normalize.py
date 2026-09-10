"""Connector 边界数据归一化（统一数据口径）

这是「数据口径收敛到 Connector 边界」的唯一入口。所有比率/金额类字段在
Connector 取数层就在这里归一化，下游 Skills / Agents 不得再自行做 /100 或 *100。

约定（务必遵守）：
  - 比率 / 百分比类字段（ROE、毛利率、净利率、资产负债率、增长率等）
    一律返回 **0–100 的百分数**（例如 10.5 表示 10.5%）。
    上游（akshare 等）财务抽象接口返回的比率本就是 0–100 百分数（如 "10.52"），
    本函数只做「去百分号 + 类型化」，不再除以或乘以 100。
  - 金额类字段（营收、净利润、市值、股东权益、现金流等）
    一律返回 **元（float）**。原始 "272.43亿" → 2.7243e10，"12.5万" → 125000。

历史坑：market_data_sources 曾用本地 `_pct` 把比率除以 100 变成 0–1 小数，
而另一些路径直接透传 / 乘 100，导致同一 `roe` 字段在不同 Connector 路径下量纲
不一致（0–1 vs 0–100），下游 fund_skill / industry_skill / valuation_agent 按单一
口径消费，产生静默的 100 倍误差。统一收敛到本模块后该问题消除。
"""
from typing import Any

# 金额单位的倍率映射（万亿 / 亿 / 万）
# 注意顺序：必须把更长的「万亿」放在前面，否则 "1.5万亿" 会被 "亿" 先匹配而失败。
_AMOUNT_UNIT_MULT = {
    "万亿": 1e12,
    "亿": 1e8,
    "万": 1e4,
}

# 视为「空 / 非法」的原始字符串
_INVALID_TOKENS = {"", "False", "None", "nan", "-", "--", "—", "无", "空"}


def to_percent(raw: Any) -> float:
    """比率 / 百分比字段 → 0–100 的百分数。

    "10.5%" / "10.5" / 10.5  → 10.5
    空字符串 / None / 非法   → 0.0
    """
    if raw is None:
        return 0.0
    s = str(raw).replace("%", "").strip()
    if s in _INVALID_TOKENS:
        return 0.0
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def to_amount_yuan(raw: Any) -> float:
    """金额字段 → 元（float）。

    "272.43亿"  → 27243000000.0
    "12.5万"    → 125000.0
    "1000000000"→ 1000000000.0（已是元，原样保留）
    空 / 非法   → 0.0
    """
    if raw is None:
        return 0.0
    s = str(raw).strip()
    if s in _INVALID_TOKENS:
        return 0.0
    try:
        # 纯数字（已是元）：直接转换
        return float(s)
    except (ValueError, TypeError):
        pass
    # 含单位（亿 / 万）：拆分数值与单位
    try:
        # 先剥离千分位逗号与"元/股"等尾缀字符，否则 float() 解析失败被误判为 0
        s = s.replace(",", "").rstrip("元股")
        for unit, mult in _AMOUNT_UNIT_MULT.items():
            if unit in s:
                num = float(s.replace(unit, "").strip())
                return num * mult
        # 兜底：剔除所有非数字/小数点/负号字符
        import re
        cleaned = re.sub(r"[^\d.\-]", "", s)
        return float(cleaned) if cleaned not in ("", "-", ".") else 0.0
    except (ValueError, TypeError):
        return 0.0


def latest_finance_record(finance_list) -> dict:
    """从财务记录列表中取最新报告期的记录（顺序无关）。

    以 report_date / end_date 字符串最大者为最新；记录无日期字段时
    退回列表首条（本地 API 按报告期降序返回，[0] 即最新）。
    历史坑：多处代码用 finance_list[-1] 取"最新"，但本地 API 是降序，
    [-1] 实际取到 3 年前最旧一期，导致 ROE/毛利率等引用过期报告期。
    """
    if not finance_list:
        return {}
    dicts = [r for r in finance_list if isinstance(r, dict)]
    if not dicts:
        return {}

    def _date_key(rec: dict) -> str:
        return str(rec.get("report_date") or rec.get("end_date") or "")

    best = max(dicts, key=_date_key)
    if _date_key(best):
        return best
    return dicts[0]


def finance_report_date(finance_list) -> str:
    """取财务记录列表最新报告期日期（YYYY-MM-DD），无数据返回空串。"""
    rec = latest_finance_record(finance_list)
    return str(rec.get("report_date") or rec.get("end_date") or "")
