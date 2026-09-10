"""数字幻觉校验器 —— 验证 Agent 报告中的数字是否来自真实数据上下文

核心逻辑：
1. 从数据上下文（Skill 层计算的真实数据）提取"合法数字池"
2. 从报告的自由文本字段（thesis / key_signals / risk_factors / recommendation）提取全部数字
3. 逐一匹配验证（允许舍入容差），计算幻觉率
4. 幻觉率超阈值时生成修正反馈，供 Agent 重试时修正

设计约定：
- overall_score / confidence 等 0-100 自产数字不校验（结构化字段，非自由文本）
- 时间段引用（近5日/30日）、日期、股票代码、指标名（MA5）等非数据数字在两侧均屏蔽
- 匹配容差：绝对 0.051 或相对 1%（覆盖正常的舍入书写，如 9.85% 写成 9.9%）
- 单位换算：数据源以元/股为单位存储大数（如 606792314），LLM 习惯以亿/万书写
  （如"融资余额6.06亿元"），数字池对 ≥1e4 的值自动追加 /1e4 与 /1e8 换算候选
- 单位错位检测（critical）：报告数字与池中值恰好相差 10^k 倍（k=±1..3）且数字
  后紧跟金额单位（亿/万/元）时，判定为换算错误（如"56.39亿" vs 数据 5.6394 亿），
  无论总幻觉率多少都判不通过，强制修正重试
- 数据池为空或报告数字少于 2 个时跳过校验（避免误杀）
"""

import re
import logging
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional, Set

logger = logging.getLogger("HallucinationValidator")

# 默认幻觉率阈值：超过则判定不通过，触发修正重试
DEFAULT_HALLUCINATION_THRESHOLD = 0.20

# 报告侧需要校验数字的自由文本字段
REPORT_TEXT_FIELDS = ("thesis", "recommendation")
REPORT_LIST_FIELDS = ("key_signals", "risk_factors")

# 两侧均屏蔽的"非数据"数字模式（时间段/日期/报告期/代码/指标名/步骤号/分数）
_MASK_PATTERNS = [
    r"\d{4}-\d{1,2}-\d{1,2}",                    # 日期：2024-08-16
    r"\d{4}(?:Q[1-4]|H[1-2]|第[一二三四1-4]季度|一季报|半年报|三季报|年报|中报)",
                                                  # 报告期：2026Q2 / 2026H1 / 2026年第2季度 / 2026年报
                                                  # （prompt 规则要求 LLM 标注报告期口径，年份部分不得判为幻觉）
    r"\d{4}年\d{1,2}月",                          # 日期：2024年8月
    r"\d{4}年",                                   # 年份：2024年
    r"(?<![\d.])\d{6}(?![\d.])",                 # 股票代码：300750（仅独立6位，避免误屏蔽大数中段）
    r"[A-Za-z]+\d+",                              # 指标名：MA5 / KDJ9 / PE100
    r"\d+(?:\.\d+)?(?:日|天|周|个月|年|季度|季)",   # 时间段：近5日 / 30日 / 3季度
    r"\d+(?:\.\d+)?个",                           # 数量短语：3个季度
    r"\d+(?:\.\d+)?个百分点",                      # 百分点差值：高10个百分点
    r"\d+(?:\.\d+)?%\s*[-–—~]\s*\d+(?:\.\d+)?%",  # 百分比区间：30%-70%（方法论阈值，非数据）
    r"Step\s*\d+",                                # 推理步骤：Step 1
]

_MASK_RE = re.compile("|".join(f"(?:{p})" for p in _MASK_PATTERNS))

# 分数形式 "45/100"、"7/9"：保留分子（评分可被报告引用），丢弃分母（量纲刻度）
_FRACTION_RE = re.compile(r"(\d+(?:\.\d+)?)/(\d+)")

# 数字提取（含负号、千分位逗号、小数）
_NUMBER_RE = re.compile(r"-?\d[\d,]*(?:\.\d+)?")


def _mask_non_data_numbers(text: str) -> str:
    """将非数据类数字（日期/时间段/代码/指标名）替换为占位符，避免误提取"""
    # 先处理分数：保留分子、丢弃分母（评分 45/100 → 45；F-Score 7/9 → 7）
    text = _FRACTION_RE.sub(r"\1", text or "")
    return _MASK_RE.sub(lambda m: "#" * len(m.group(0)), text)


def extract_numbers(text: str) -> List[float]:
    """从文本中提取真实数据数字（已屏蔽非数据模式），保持出现顺序"""
    if not text:
        return []
    masked = _mask_non_data_numbers(text)
    numbers = []
    for match in _NUMBER_RE.finditer(masked):
        raw = match.group(0).replace(",", "")
        try:
            numbers.append(float(raw))
        except ValueError:
            continue
    return numbers


def build_number_pool(data_context: str) -> Set[float]:
    """从数据上下文构建合法数字池（含万/亿/万亿单位换算候选值）

    数据源通常以元/股为单位存储大数（如 rz_balance=606792314），
    LLM 报告习惯以亿/万为单位书写（如"融资余额6.06亿元"）。
    对 ≥1e4 的值追加 /1e4（万）、/1e8（亿）、/1e12（万亿）换算候选，
    避免正确的单位换算被误判为幻觉。容差仍为 1%，跨数量级不会误匹配。
    """
    pool = set(extract_numbers(data_context))
    scaled = set()
    for p in pool:
        if abs(p) >= 1e4:
            scaled.add(round(p / 1e4, 4))
            scaled.add(round(p / 1e8, 4))
            scaled.add(round(p / 1e12, 4))
    pool |= scaled
    return pool


def _matches_pool(value: float, pool: Set[float]) -> bool:
    """判断报告数字是否可匹配到数据池中的某个数字（含舍入容差）"""
    for p in pool:
        tolerance = max(0.051, abs(p) * 0.01)
        if abs(value - p) <= tolerance:
            return True
    return False


# 单位错位检测的量级因子（10^±k，k=1..3）
_MAGNITUDE_FACTORS = (0.001, 0.01, 0.1, 10.0, 100.0, 1000.0)
# 金额单位标记：仅当数字后 1-2 字符内含这些标记时才启用错位检测
# （把检测范围限定在金额语境，避免"3连阴"等小整数 ×10 撞上池中无关数字的误报）
_UNIT_MARKERS = ("亿", "万", "元")


def _has_unit_marker(text: str, pos: int) -> bool:
    """数字结束位置 pos 之后 1-2 字符内是否含金额单位标记"""
    tail = text[pos:pos + 2]
    return any(m in tail for m in _UNIT_MARKERS)


def _magnitude_match(value: float, pool: Set[float]):
    """单位错位检测：value×10^k 后命中池值则返回 (池值, 因子)，否则 None

    实测案例：数据 rz_balance=563939375（池含亿候选 5.6394），LLM 误写
    "融资余额56.39亿元"（÷1e7 而非 ÷1e8），56.39×0.1=5.639 命中 → 判定换算错误。
    """
    if value == 0:
        return None
    for factor in _MAGNITUDE_FACTORS:
        scaled = value * factor
        for p in pool:
            tolerance = max(0.051, abs(p) * 0.01)
            if abs(scaled - p) <= tolerance:
                return (p, factor)
    return None


@dataclass
class HallucinationCheck:
    """数字幻觉校验结果"""

    total_numbers: int = 0
    verified_count: int = 0
    hallucination_rate: float = 0.0
    unverifiable: List[Dict[str, Any]] = field(default_factory=list)
    # 单位错位（与池值恰好差 10^k 倍）的数字个数——单个即判不通过，
    # 因为 10 倍量级的金额错误比多个小误差更严重（总幻觉率会被低占比稀释）
    critical_count: int = 0
    threshold: float = DEFAULT_HALLUCINATION_THRESHOLD
    skipped: bool = False
    skip_reason: str = ""

    @property
    def passed(self) -> bool:
        if self.skipped:
            return True
        if self.critical_count > 0:
            return False
        return self.hallucination_rate <= self.threshold

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["passed"] = self.passed
        return d


def validate_report_numbers(
    report: Dict[str, Any],
    data_context: str,
    threshold: float = DEFAULT_HALLUCINATION_THRESHOLD,
) -> HallucinationCheck:
    """校验报告自由文本中的数字是否都能在数据上下文中找到

    Args:
        report: AgentReport.to_dict() 形式的报告
        data_context: 喂给 LLM 的数据上下文（合法数字来源）
        threshold: 幻觉率阈值（0-1），超过则 passed=False
    """
    check = HallucinationCheck(threshold=threshold)

    pool = build_number_pool(data_context)
    if not pool:
        check.skipped = True
        check.skip_reason = "数据上下文无有效数字，跳过校验"
        return check

    # 收集报告自由文本字段（带字段名定位，便于修正反馈）
    texts: List[tuple] = []
    for f in REPORT_TEXT_FIELDS:
        if report.get(f):
            texts.append((f, str(report[f])))
    for f in REPORT_LIST_FIELDS:
        items = report.get(f)
        if isinstance(items, list):
            for i, item in enumerate(items):
                if item:
                    texts.append((f"[{i}]", str(item)))

    for field_name, text in texts:
        masked_text = _mask_non_data_numbers(text)
        for match in _NUMBER_RE.finditer(masked_text):
            raw = match.group(0).replace(",", "")
            try:
                value = float(raw)
            except ValueError:
                continue
            check.total_numbers += 1
            if _matches_pool(value, pool):
                check.verified_count += 1
            else:
                entry = {"value": value, "raw": raw, "field": field_name, "text": text[:80]}
                # 单位错位检测：仅对带金额单位（亿/万/元）的数字启用，
                # 数字与池值恰好差 10^k 倍 → 判定换算错误（critical）
                if _has_unit_marker(masked_text, match.end()):
                    mag = _magnitude_match(value, pool)
                    if mag:
                        pool_value, factor = mag
                        entry["critical"] = True
                        entry["magnitude_match"] = {
                            "pool_value": pool_value,
                            "factor": factor,
                            "hint": (f"报告值是数据值{pool_value}的{1/factor:.0f}倍"
                                     if factor < 1 else
                                     f"报告值是数据值{pool_value}的1/{factor:.0f}"),
                        }
                        check.critical_count += 1
                check.unverifiable.append(entry)

    if check.total_numbers < 2 and check.critical_count == 0:
        check.skipped = True
        check.skip_reason = f"报告数字过少（{check.total_numbers}个），跳过校验"
        return check

    check.hallucination_rate = round(
        len(check.unverifiable) / check.total_numbers, 4
    )
    return check


def build_correction_feedback(check: HallucinationCheck) -> str:
    """生成修正重试的反馈文本，追加到原 prompt 之后"""
    lines = [
        "",
        "⚠ 数据核验反馈（上一版报告未通过数字校验）：",
        f"幻觉率 {check.hallucination_rate:.0%}（阈值 {check.threshold:.0%}），"
        f"以下 {len(check.unverifiable)} 个数字未在数据上下文中找到：",
    ]
    for item in check.unverifiable[:8]:
        if item.get("critical"):
            hint = item.get("magnitude_match", {}).get("hint", "")
            lines.append(
                f'- "{item["raw"]}"（位于 {item["field"]}）⚠ 疑似单位换算错误：{hint}。'
                f"请核对换算（亿=元÷1e8，万=元÷1e4）后改写"
            )
        else:
            lines.append(f'- "{item["raw"]}"（位于 {item["field"]}）')
    lines.append(
        "请严格基于数据上下文中的真实数字重新输出完整JSON报告："
        "未通过校验的数字必须替换为数据中的真实值，或改写为不含数字的定性表述后删除该数字；"
        "禁止保留任何数据中不存在的数字。"
    )
    return "\n".join(lines)
