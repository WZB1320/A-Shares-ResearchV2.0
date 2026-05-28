"""多智能体辩论引擎 — Phase 2 交叉审阅 + Phase 3 修订轮"""
import json
import logging
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.agents.report_schema import AgentReport, parse_json_report

logger = logging.getLogger("DebateEngine")

REVIEW_MAX_TOKENS = 800
REVISE_MAX_TOKENS = 1200
LLM_TEMPERATURE = 0.0

CROSS_PAIRINGS = [
    ("tech", "fund"),
    ("fund", "risk"),
    ("risk", "valuation"),
    ("valuation", "tech"),
    ("capital", "industry"),
    ("industry", "capital"),
]

DIM_LABELS = {
    "tech": "技术面", "fund": "基本面", "capital": "资金面",
    "industry": "行业面", "risk": "风险面", "valuation": "估值面",
}


# ==================== 数据结构 ====================

@dataclass
class ReviewOpinion:
    reviewer_dim: str
    target_dim: str
    is_contradiction: bool
    contradiction_detail: str
    suggested_revision: str
    agree_points: str


@dataclass
class RevisionResult:
    dimension: str
    original_score: int
    original_grade: str
    revised_score: int
    revised_grade: str
    changed: bool
    reason: str
    revised_report: Optional[AgentReport] = None


# ==================== 审阅函数 ====================

def cross_review(
    reviewer_dim: str,
    target_report: AgentReport,
    model_name: str = DEFAULT_MODEL,
) -> ReviewOpinion:
    """
    Phase 2: Agent A 审阅 Agent B 的初版报告，找出潜在矛盾
    """
    client = get_llm_client(model_name)
    reviewer_label = DIM_LABELS.get(reviewer_dim, reviewer_dim)
    target_label = DIM_LABELS.get(target_report.dimension, target_report.dimension)

    prompt = f"""你是一位资深{reviewer_label}分析师，现在需要审阅一份来自{target_label}分析师的投资分析报告，找出两者之间潜在的逻辑矛盾。

【{target_label}分析报告 — 来自 {target_report.dimension}Agent】
- 综合评分: {target_report.overall_score}/100 ({target_report.grade})
- 核心判断: {target_report.thesis}
- 关键信号: {json.dumps(target_report.key_signals, ensure_ascii=False)}
- 风险因素: {json.dumps(target_report.risk_factors, ensure_ascii=False)}
- 操作建议: {target_report.recommendation}

请从{reviewer_label}分析师的视角审阅这份报告，重点关注：
1. {target_label}的判断与{reviewer_label}通常能观察到的市场信号是否一致？
2. 如果{target_label}认为基本面优秀，但{reviewer_label}层面可能出现什么矛盾信号？
3. {target_label}的风险提示是否遗漏了{reviewer_label}视角下的风险点？

请严格按以下JSON格式输出（不要输出其他任何内容）：

{{
  "is_contradiction": true/false,
  "contradiction_detail": "具体矛盾描述（如无矛盾写"未发现明显矛盾"）",
  "suggested_revision": "建议{target_label}Agent修改的方向（评分调整建议、逻辑修正等）",
  "agree_points": "你认可的{target_label}报告中合理的部分"
}}"""

    try:
        completion = client.chat.completions.create(
            model=get_model_id(model_name),
            messages=[
                {"role": "system", "content": "你是一位严谨的交叉审阅分析师。请客观审视报告，找出潜在逻辑矛盾，不要怀疑自己的专业判断。"},
                {"role": "user", "content": prompt},
            ],
            temperature=LLM_TEMPERATURE,
            max_tokens=REVIEW_MAX_TOKENS,
        )
        raw = completion.choices[0].message.content.strip()

        for prefix, suffix in [("{", "}"), ("```json\n", "\n```"), ("```json", "```"), ("```\n", "\n```")]:
            if prefix == "{":
                start = raw.find("{")
                end = raw.rfind("}")
                if start != -1 and end > start:
                    try:
                        obj = json.loads(raw[start:end + 1])
                        return ReviewOpinion(
                            reviewer_dim=reviewer_dim,
                            target_dim=target_report.dimension,
                            is_contradiction=obj.get("is_contradiction", False),
                            contradiction_detail=obj.get("contradiction_detail", ""),
                            suggested_revision=obj.get("suggested_revision", ""),
                            agree_points=obj.get("agree_points", ""),
                        )
                    except json.JSONDecodeError:
                        pass
            else:
                if raw.startswith(prefix) and raw.endswith(suffix):
                    inner = raw[len(prefix):-len(suffix)].strip()
                    try:
                        obj = json.loads(inner)
                        return ReviewOpinion(
                            reviewer_dim=reviewer_dim,
                            target_dim=target_report.dimension,
                            is_contradiction=obj.get("is_contradiction", False),
                            contradiction_detail=obj.get("contradiction_detail", ""),
                            suggested_revision=obj.get("suggested_revision", ""),
                            agree_points=obj.get("agree_points", ""),
                        )
                    except json.JSONDecodeError:
                        pass

        return ReviewOpinion(
            reviewer_dim=reviewer_dim, target_dim=target_report.dimension,
            is_contradiction=False, contradiction_detail="",
            suggested_revision="", agree_points=raw[:200],
        )
    except Exception as e:
        logger.error(f"[Debate] 交叉审阅失败 ({reviewer_dim}→{target_report.dimension}): {e}")
        return ReviewOpinion(
            reviewer_dim=reviewer_dim, target_dim=target_report.dimension,
            is_contradiction=False, contradiction_detail=f"审阅异常: {e}",
            suggested_revision="", agree_points="",
        )


# ==================== 修订函数 ====================

def revise_report(
    dimension: str,
    original_report: AgentReport,
    review_opinion: ReviewOpinion,
    model_name: str = DEFAULT_MODEL,
) -> RevisionResult:
    """
    Phase 3: Agent 收到审阅意见后，决定修改评分 or 坚持原判
    返回修订后的 AgentReport + 修订理由
    """
    client = get_llm_client(model_name)
    label = DIM_LABELS.get(dimension, dimension)
    reviewer_label = DIM_LABELS.get(review_opinion.reviewer_dim, review_opinion.reviewer_dim)

    if not review_opinion.is_contradiction or not review_opinion.suggested_revision:
        logger.info(f"[Debate] {label}Agent 无需修订（{reviewer_label}Agent未发现矛盾）")
        return RevisionResult(
            dimension=dimension,
            original_score=original_report.overall_score,
            original_grade=original_report.grade,
            revised_score=original_report.overall_score,
            revised_grade=original_report.grade,
            changed=False,
            reason=f"{reviewer_label}Agent审阅后未发现需要修订的矛盾",
            revised_report=original_report,
        )

    prompt = f"""你是一位资深{label}分析师。你之前撰写了一份分析报告，现在收到了来自{reviewer_label}分析师的跨维度审阅意见。

【你的原始报告】
- 综合评分: {original_report.overall_score}/100 ({original_report.grade})
- 核心判断: {original_report.thesis}
- 关键信号: {json.dumps(original_report.key_signals, ensure_ascii=False)}
- 风险因素: {json.dumps(original_report.risk_factors, ensure_ascii=False)}
- 操作建议: {original_report.recommendation}

【{reviewer_label}Agent的审阅意见】
- 是否发现矛盾: {'是' if review_opinion.is_contradiction else '否'}
- 矛盾描述: {review_opinion.contradiction_detail}
- 修改建议: {review_opinion.suggested_revision}
- 认可的观点: {review_opinion.agree_points}

请认真思考{reviewer_label}Agent的审阅意见：
1. 审阅意见是否有道理？矛盾是否真实存在？
2. 如果确实存在逻辑漏洞，评分应该调整多少？理由是什么？
3. 如果要调整评分，核心判断需要如何修正？
4. 如果不同意审阅意见，你的反驳理由是什么？

请严格按以下JSON格式输出修订后的报告（不要输出其他任何内容）：

{{
  "changed": true/false,
  "reason": "调整理由 或 反驳理由",
  "overall_score": 整数0-100,
  "grade": "强烈看多/看多/中性偏多/中性/中性偏空/看空/强烈看空",
  "thesis": "修正后的核心判断（如未改变则与原始相同）",
  "key_signals": ["修正后的关键信号1", "修正后的关键信号2"],
  "risk_factors": ["修正后的风险因素1", "修正后的风险因素2"],
  "recommendation": "修正后的操作建议"
}}"""

    try:
        completion = client.chat.completions.create(
            model=get_model_id(model_name),
            messages=[
                {"role": "system", "content": "你是一位严谨的分析师。请虚心听取跨维度审阅意见，如果对方有道理就修改，如果没有道理就据理反驳。不要为了妥协而妥协。"},
                {"role": "user", "content": prompt},
            ],
            temperature=LLM_TEMPERATURE,
            max_tokens=REVISE_MAX_TOKENS,
        )
        raw_text = completion.choices[0].message.content.strip()

        for prefix, suffix in [("{", "}"), ("```json\n", "\n```"), ("```json", "```"), ("```\n", "\n```")]:
            if prefix == "{":
                start = raw_text.find("{")
                end = raw_text.rfind("}")
                if start != -1 and end > start:
                    try:
                        obj = json.loads(raw_text[start:end + 1])
                        changed = obj.get("changed", False)
                        revised_score = _clamp(obj.get("overall_score", original_report.overall_score), 0, 100)
                        revised_grade = obj.get("grade", original_report.grade)
                        reason = obj.get("reason", "")

                        revised = AgentReport(
                            dimension=dimension,
                            overall_score=revised_score if changed else original_report.overall_score,
                            grade=revised_grade if changed else original_report.grade,
                            confidence=original_report.confidence,
                            thesis=obj.get("thesis", original_report.thesis) if changed else original_report.thesis,
                            key_signals=obj.get("key_signals", original_report.key_signals) if changed else original_report.key_signals,
                            risk_factors=obj.get("risk_factors", original_report.risk_factors) if changed else original_report.risk_factors,
                            recommendation=obj.get("recommendation", original_report.recommendation) if changed else original_report.recommendation,
                            supporting_data=original_report.supporting_data,
                            raw_text=raw_text,
                        )
                        return RevisionResult(
                            dimension=dimension,
                            original_score=original_report.overall_score,
                            original_grade=original_report.grade,
                            revised_score=revised.overall_score,
                            revised_grade=revised.grade,
                            changed=changed,
                            reason=reason,
                            revised_report=revised,
                        )
                    except json.JSONDecodeError:
                        pass
            else:
                if raw_text.startswith(prefix) and raw_text.endswith(suffix):
                    inner = raw_text[len(prefix):-len(suffix)].strip()
                    try:
                        obj = json.loads(inner)
                        changed = obj.get("changed", False)
                        revised_score = _clamp(obj.get("overall_score", original_report.overall_score), 0, 100)
                        revised_grade = obj.get("grade", original_report.grade)
                        reason = obj.get("reason", "")

                        revised = AgentReport(
                            dimension=dimension,
                            overall_score=revised_score if changed else original_report.overall_score,
                            grade=revised_grade if changed else original_report.grade,
                            confidence=original_report.confidence,
                            thesis=obj.get("thesis", original_report.thesis) if changed else original_report.thesis,
                            key_signals=obj.get("key_signals", original_report.key_signals) if changed else original_report.key_signals,
                            risk_factors=obj.get("risk_factors", original_report.risk_factors) if changed else original_report.risk_factors,
                            recommendation=obj.get("recommendation", original_report.recommendation) if changed else original_report.recommendation,
                            supporting_data=original_report.supporting_data,
                            raw_text=raw_text,
                        )
                        return RevisionResult(
                            dimension=dimension,
                            original_score=original_report.overall_score,
                            original_grade=original_report.grade,
                            revised_score=revised.overall_score,
                            revised_grade=revised.grade,
                            changed=changed,
                            reason=reason,
                            revised_report=revised,
                        )
                    except json.JSONDecodeError:
                        pass

        logger.warning(f"[Debate] 修订JSON解析失败 ({dimension})，保留原报告")
        return RevisionResult(
            dimension=dimension,
            original_score=original_report.overall_score,
            original_grade=original_report.grade,
            revised_score=original_report.overall_score,
            revised_grade=original_report.grade,
            changed=False,
            reason=f"LLM修订输出JSON解析失败，保留原始判断",
            revised_report=original_report,
        )
    except Exception as e:
        logger.error(f"[Debate] 修订失败 ({dimension}): {e}")
        return RevisionResult(
            dimension=dimension,
            original_score=original_report.overall_score,
            original_grade=original_report.grade,
            revised_score=original_report.overall_score,
            revised_grade=original_report.grade,
            changed=False,
            reason=f"修订异常: {e}",
            revised_report=original_report,
        )


# ==================== 辩论过程中控 ====================

def run_debate_rounds(
    phase1_reports: Dict[str, AgentReport],
    model_name: str = DEFAULT_MODEL,
) -> tuple:
    """
    执行两阶段辩论流程，返回 (修订后的最终报告, 修订结果详情)
    """
    logger.info("[Debate] ====== Phase 2: 交叉审阅轮 ======")

    reviews: Dict[str, ReviewOpinion] = {}
    for reviewer, target in CROSS_PAIRINGS:
        if target not in phase1_reports:
            logger.info(f"[Debate] 跳过 {reviewer}→{target}（目标Agent未参与）")
            continue

        target_report = phase1_reports[target]
        if target_report.parse_error:
            logger.info(f"[Debate] 跳过 {reviewer}→{target}（目标报告解析失败）")
            continue

        logger.info(f"[Debate] {DIM_LABELS.get(reviewer, reviewer)} 审阅 {DIM_LABELS.get(target, target)}")
        opinion = cross_review(reviewer, target_report, model_name)
        reviews[target] = opinion

    logger.info(f"[Debate] ====== Phase 3: 修订轮 ({len(reviews)} 个Agent收到审阅意见) ======")

    results: Dict[str, RevisionResult] = {}
    for dim, opinion in reviews.items():
        original = phase1_reports[dim]
        label = DIM_LABELS.get(dim, dim)
        logger.info(f"[Debate] {label}Agent 修订中...")
        result = revise_report(dim, original, opinion, model_name)
        results[dim] = result
        if result.changed:
            logger.info(
                f"[Debate] {label}Agent 评分调整: "
                f"{result.original_score}→{result.revised_score} "
                f"({result.original_grade}→{result.revised_grade})"
            )
        else:
            logger.info(f"[Debate] {label}Agent 坚持原判: {result.reason[:60]}")

    final_reports = {}
    for dim, report in phase1_reports.items():
        if dim in results:
            final_reports[dim] = results[dim].revised_report if results[dim].revised_report else report
        else:
            final_reports[dim] = report

    return final_reports, results


def build_debate_summary(
    phase1_reports: Dict[str, AgentReport],
    final_results: Dict[str, RevisionResult],
) -> str:
    """
    构建 Agent分歧摘要 的 Markdown 文本
    """
    if not final_results:
        return ""

    lines = ["## Agent分歧与修订摘要", ""]
    any_changed = any(r.changed for r in final_results.values())

    lines.append("| 维度 | 原始评分 | 修订评分 | 变化 | 修订/反驳理由 |")
    lines.append("|------|----------|----------|------|--------------|")

    for dim, result in final_results.items():
        label = DIM_LABELS.get(dim, dim)
        changed_mark = f"[*] {result.original_score}->{result.revised_score}" if result.changed else "  未变动"
        reason_short = result.reason[:80] + ("..." if len(result.reason) > 80 else "")
        lines.append(
            f"| {label} | {result.original_score} ({result.original_grade}) | "
            f"{result.revised_score} ({result.revised_grade}) | "
            f"{changed_mark} | {reason_short} |"
        )

    lines.append("")

    if any_changed:
        lines.append("### 评分变动详情")
        lines.append("")
        for dim, result in final_results.items():
            if result.changed:
                label = DIM_LABELS.get(dim, dim)
                lines.append(f"**{label}Agent**")
                lines.append(f"- 原始: {result.original_score}/100 ({result.original_grade})")
                lines.append(f"- 修订: {result.revised_score}/100 ({result.revised_grade})")
                lines.append(f"- 理由: {result.reason}")
                lines.append("")
    else:
        lines.append("> 本轮辩论中各Agent均坚持原始判断，未发现需要修订的矛盾。")
        lines.append("")

    lines.append("### 跨维度审阅配对")
    lines.append("")
    for reviewer, target in CROSS_PAIRINGS:
        rl = DIM_LABELS.get(reviewer, reviewer)
        tl = DIM_LABELS.get(target, target)
        lines.append(f"- **{rl}** 审阅 **{tl}**")

    return "\n".join(lines)


def _clamp(val: int, low: int, high: int) -> int:
    return max(low, min(high, val))