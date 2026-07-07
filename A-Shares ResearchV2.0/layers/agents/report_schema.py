import json
import re
import logging
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any, Tuple

logger = logging.getLogger("ReportSchema")

GRADE_WEIGHTS = {
    "强烈看多": 100,
    "看多": 75,
    "中性偏多": 62,
    "中性": 50,
    "中性偏空": 38,
    "看空": 25,
    "强烈看空": 0,
}

GRADE_REVERSE = {v: k for k, v in sorted(GRADE_WEIGHTS.items(), key=lambda x: x[1])}


@dataclass
class AgentReport:
    dimension: str
    overall_score: int
    grade: str
    confidence: int
    thesis: str
    key_signals: List[str]
    risk_factors: List[str]
    recommendation: str
    supporting_data: Dict[str, Any] = field(default_factory=dict)
    raw_text: str = ""
    parse_error: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def is_valid(self) -> bool:
        return not self.parse_error and self.thesis != ""

    @classmethod
    def from_dict(cls, d: Dict[str, Any], dimension: str = "unknown") -> "AgentReport":
        return cls(
            dimension=d.get("dimension", dimension),
            overall_score=_safe_int(d.get("overall_score"), 50),
            grade=_safe_str(d.get("grade"), "中性"),
            confidence=_safe_int(d.get("confidence"), 50),
            thesis=_safe_str(d.get("thesis"), d.get("summary", "")),
            key_signals=_safe_str_list(d.get("key_signals", [])),
            risk_factors=_safe_str_list(d.get("risk_factors", [])),
            recommendation=_safe_str(d.get("recommendation"), ""),
            supporting_data=d.get("supporting_data", {}) if isinstance(d.get("supporting_data"), dict) else {},
            raw_text="",
            parse_error=False,
        )


def _safe_int(val: Any, default: int = 0) -> int:
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return default


def _safe_str(val: Any, default: str = "") -> str:
    if val is None:
        return default
    return str(val)


def _safe_str_list(val: Any) -> List[str]:
    if not isinstance(val, list):
        return []
    return [str(item) for item in val[:8]]


def parse_json_report(raw_text: str, dimension: str) -> AgentReport:
    if not raw_text or not raw_text.strip():
        return _empty_report(dimension, "LLM返回空响应")

    text = raw_text.strip()

    json_obj = _extract_json(text)

    if json_obj is not None:
        try:
            report = AgentReport.from_dict(json_obj, dimension)
            report.raw_text = text
            return report
        except Exception as e:
            logger.warning(f"[ReportSchema] JSON解析后构造失败 ({dimension}): {e}")

    return _fallback_text_report(text, dimension)


def _extract_json(text: str) -> Optional[Dict]:
    for prefix, suffix in [("{", "}"), ("```json\n", "\n```"), ("```json", "```"), ("```\n", "\n```")]:
        if prefix == "{":
            start = text.find("{")
            end = text.rfind("}")
            if start != -1 and end > start:
                try:
                    return json.loads(text[start:end + 1])
                except json.JSONDecodeError:
                    continue
        else:
            if text.startswith(prefix) and text.endswith(suffix):
                inner = text[len(prefix):-len(suffix)].strip()
                try:
                    return json.loads(inner)
                except json.JSONDecodeError:
                    continue

    bare_match = re.search(r'\{[^{}]*\{[^{}]*\}[^{}]*\}|\{[^{}]*\}', text, re.DOTALL)
    if bare_match:
        try:
            return json.loads(bare_match.group(0))
        except json.JSONDecodeError:
            pass

    bold_match = re.search(r'\*\*\{[^{}]*\}\*\*', text)
    if bold_match:
        inner = bold_match.group(0).strip("*")
        try:
            return json.loads(inner)
        except json.JSONDecodeError:
            pass

    return None


def _fallback_text_report(text: str, dimension: str) -> AgentReport:
    report = _empty_report(dimension, "")
    report.parse_error = True
    report.raw_text = text
    report.thesis = _extract_first_meaningful_line(text)
    return report


def _empty_report(dimension: str, reason: str) -> AgentReport:
    return AgentReport(
        dimension=dimension,
        overall_score=50,
        grade="中性",
        confidence=0,
        thesis=reason or "该维度分析不可用",
        key_signals=[],
        risk_factors=[],
        recommendation="数据不足，无法给出建议",
        supporting_data={},
        raw_text="",
        parse_error=True,
    )


def _extract_first_meaningful_line(text: str) -> str:
    lines = [l.strip() for l in text.split("\n") if l.strip() and not l.strip().startswith("```")]
    for line in lines:
        clean = re.sub(r'^#+\s*', '', line).strip()
        if len(clean) > 10:
            return clean[:200]
    return text[:200] if text else ""


def error_report(dimension: str, error_msg: str) -> AgentReport:
    return AgentReport(
        dimension=dimension,
        overall_score=50,
        grade="中性",
        confidence=0,
        thesis=f"分析异常: {error_msg[:100]}",
        key_signals=[],
        risk_factors=[],
        recommendation="该维度分析失败，请检查数据",
        supporting_data={},
        raw_text=error_msg,
        parse_error=True,
    )


def unavailable_report(dimension: str) -> AgentReport:
    return AgentReport(
        dimension=dimension,
        overall_score=50,
        grade="中性",
        confidence=0,
        thesis="数据不可用，该维度无法分析",
        key_signals=[],
        risk_factors=[],
        recommendation="数据不可用，无法给出建议",
        supporting_data={},
        raw_text="",
        parse_error=True,
    )


def aggregate_reports(reports: Dict[str, AgentReport]) -> Dict[str, Any]:
    valid_reports = {k: v for k, v in reports.items() if v.is_valid()}
    invalid_reports = {k: v for k, v in reports.items() if not v.is_valid()}

    if not valid_reports:
        return {
            "overall_score": 50,
            "overall_grade": "中性",
            "overall_confidence": 0,
            "dimension_count": 0,
            "valid_count": 0,
            "consensus": "无有效数据，无法判断",
            "dimensions": {},
            "all_signals": [],
            "all_risks": [],
            "conflicts": [],
        }

    dim_scores = {}
    dim_grades = {}
    all_signals = []
    all_risks = []
    conflicts = []

    for dim_name, report in valid_reports.items():
        dim_scores[dim_name] = {
            "score": report.overall_score,
            "grade": report.grade,
            "confidence": report.confidence,
            "thesis": report.thesis,
            "recommendation": report.recommendation,
        }
        all_signals.extend(report.key_signals[:3])
        all_risks.extend(report.risk_factors[:3])

    grades_and_weights = [
        (report.grade, GRADE_WEIGHTS.get(report.grade, 50), report.confidence)
        for report in valid_reports.values()
    ]

    valid_weights = [(g, w, c) for g, w, c in grades_and_weights if g in GRADE_WEIGHTS]

    if valid_weights:
        total_weight = sum(c for _, _, c in valid_weights)
        if total_weight > 0:
            weighted_score = sum(w * c for _, w, c in valid_weights) / total_weight
        else:
            weighted_score = sum(w for _, w, _ in valid_weights) / len(valid_weights)
    else:
        weighted_score = 50

    overall_score = round(weighted_score)
    overall_grade = _score_to_grade(overall_score)

    avg_confidence = sum(r.confidence for r in valid_reports.values()) // max(len(valid_reports), 1)

    grade_values = [GRADE_WEIGHTS.get(r.grade, 50) for r in valid_reports.values()]
    if len(grade_values) >= 2 and max(grade_values) - min(grade_values) >= 50:
        conflicts.append("多空分歧显著：不同维度信号方向不一致")

    consensus = "多空分歧，需谨慎判断" if conflicts else _build_consensus(grade_values)

    report_counts = {}
    for r in valid_reports.values():
        report_counts[r.grade] = report_counts.get(r.grade, 0) + 1

    # 投票分布：偏多/中性/偏空 三档票数 + 对应维度名
    bullish_grades = ("强烈看多", "看多", "中性偏多")
    bearish_grades = ("中性偏空", "看空", "强烈看空")
    dim_names_map = {"tech": "技术面", "fund": "基本面", "capital": "资金面",
                     "industry": "行业面", "risk": "风险面", "valuation": "估值面"}
    vote_distribution = {"偏多": 0, "中性": 0, "偏空": 0}
    vote_dims = {"偏多": [], "中性": [], "偏空": []}
    for dim_key, r in valid_reports.items():
        dim_label = dim_names_map.get(dim_key, dim_key)
        if r.grade in bullish_grades:
            vote_distribution["偏多"] += 1
            vote_dims["偏多"].append(dim_label)
        elif r.grade in bearish_grades:
            vote_distribution["偏空"] += 1
            vote_dims["偏空"].append(dim_label)
        else:
            vote_distribution["中性"] += 1
            vote_dims["中性"].append(dim_label)

    return {
        "overall_score": overall_score,
        "overall_grade": overall_grade,
        "overall_confidence": avg_confidence,
        "dimension_count": len(reports),
        "valid_count": len(valid_reports),
        "consensus": consensus,
        "dimensions": dim_scores,
        "all_signals": all_signals[:10],
        "all_risks": all_risks[:10],
        "conflicts": conflicts,
        "invalid_dimensions": {k: v.thesis for k, v in invalid_reports.items()},
        "vote_distribution": vote_distribution,
        "vote_dims": vote_dims,
    }


def _score_to_grade(score: int) -> str:
    thresholds = [(90, "强烈看多"), (70, "看多"), (58, "中性偏多"),
                  (42, "中性"), (30, "中性偏空"), (15, "看空")]
    for threshold, grade in thresholds:
        if score >= threshold:
            return grade
    return "强烈看空"


def _build_consensus(grade_values: List[int]) -> str:
    if not grade_values:
        return "无有效数据"
    avg = sum(grade_values) / len(grade_values)
    if avg >= 80:
        return "高度一致看多"
    elif avg >= 65:
        return "普遍偏多"
    elif avg >= 45:
        return "观点中性"
    elif avg >= 30:
        return "普遍偏空"
    else:
        return "高度一致看空"


def reports_to_markdown(reports: Dict[str, AgentReport]) -> str:
    dim_names = {"tech": "技术面", "fund": "基本面", "capital": "资金面",
                 "industry": "行业面", "risk": "风险面", "valuation": "估值面"}

    parts = []
    for key, report in reports.items():
        name = dim_names.get(key, key)
        if report.parse_error:
            if report.thesis and "不可用" in report.thesis:
                parts.append(f"### {name}分析\n⚠️ 数据不可用\n")
            else:
                parts.append(f"### {name}分析\n⚠️ 结构化解析失败，以下为原始分析:\n\n{report.raw_text[:800]}\n")
            continue

        parts.append(f"""### {name}分析

**核心判断**: {report.thesis}
**评分**: {report.overall_score}/100 | **评级**: {report.grade} | **置信度**: {report.confidence}%

**关键信号**:
{chr(10).join(f'- {s}' for s in report.key_signals)}

**风险因素**:
{chr(10).join(f'- {r}' for r in report.risk_factors)}

**建议**: {report.recommendation}
""")

    return "\n\n".join(parts)