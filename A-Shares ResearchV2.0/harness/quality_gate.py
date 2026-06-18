"""质量门禁模块 - 评估 Agent 报告质量，低分触发重试

从 graph/workflow.py 提取，作为 ChiefAgent 的统一质量保障阶段。
所有执行路径（ChiefAgent / scheduler）共用同一套质量标准。

评分维度（满分100）：
  - 长度分 (0-30)：≥800字→30, ≥500→20, ≥200→10, 否则5
  - 结构分 (0-25)：含标题/编号→25, 含换行→10
  - 数据引用分 (0-25)：≥5个数字+单位→25, ≥2→15, ≥1→8
  - 结论分 (0-20)：≥4个判断词→20, ≥2→12, ≥1→5
"""
import re
import logging
from typing import Dict, Any, Optional, List

logger = logging.getLogger("QualityGate")

# 质量门禁阈值
QUALITY_MIN_SCORE = 60       # 低于此分触发重试（原 workflow.py 为 50）
QUALITY_MIN_LENGTH = 200     # 最低字符数
MAX_RETRY_PER_AGENT = 2      # 每个 Agent 最多重试次数（原 workflow.py 为 1）

# 判断性关键词
JUDGMENT_KEYWORDS = [
    '看多', '看空', '震荡', '买入', '卖出', '持有', '增持', '减持',
    '建议', '评级', '评分', '综合', '推荐', '回避', '低配', '高配',
    'bullish', 'bearish', 'buy', 'sell', 'hold',
]


def score_report(report) -> float:
    """评分报告质量，支持 str / dict(AgentReport) / None

    Args:
        report: 报告内容，可为字符串、AgentReport dict 或 None

    Returns:
        质量评分（0-100）
    """
    if report is None:
        return 0.0
    if isinstance(report, dict):
        report = report.get("raw_text", "") or str(report)
    if not isinstance(report, str):
        report = str(report)
    if not report or report.startswith("⚠️"):
        return 0.0

    score = 0.0

    # 1) 长度分 (0-30)
    length = len(report)
    if length >= 800:
        score += 30
    elif length >= 500:
        score += 20
    elif length >= QUALITY_MIN_LENGTH:
        score += 10
    else:
        score += 5

    # 2) 结构分 (0-25)：检测标题/数字编号
    if re.search(r'#{1,3}\s|【.+】|\d+[\.\、]', report):
        score += 25
    elif '\n' in report:
        score += 10

    # 3) 数据引用分 (0-25)：检测数字+单位
    data_count = len(re.findall(r'\d+\.?\d*\s*[%亿万手倍]', report))
    if data_count >= 5:
        score += 25
    elif data_count >= 2:
        score += 15
    elif data_count >= 1:
        score += 8

    # 4) 结论分 (0-20)：检测判断性关键词
    match_count = sum(
        1 for kw in JUDGMENT_KEYWORDS
        if kw.lower() in report.lower()
    )
    if match_count >= 4:
        score += 20
    elif match_count >= 2:
        score += 12
    elif match_count >= 1:
        score += 5

    return score


class QualityGate:
    """质量门禁：评估报告质量，低分触发重试

    用法：
        gate = QualityGate()
        reports = gate.evaluate_and_retry(
            reports=raw_reports,
            stock_code="600519",
            all_data=all_data,
            scheduler=scheduler,
            model_name="deepseek",
        )
    """

    def __init__(
        self,
        min_score: float = QUALITY_MIN_SCORE,
        max_retry: int = MAX_RETRY_PER_AGENT,
    ):
        self.min_score = min_score
        self.max_retry = max_retry

    def evaluate_and_retry(
        self,
        reports: Dict[str, Dict],
        stock_code: str,
        all_data: Dict,
        scheduler: Optional[Any] = None,
        model_name: str = "",
    ) -> Dict[str, Dict]:
        """评估报告质量，低分触发重试

        Args:
            reports: {agent_name: report_dict} 字典
            stock_code: 股票代码
            all_data: 完整数据（用于重试时构建 data_payload）
            scheduler: HarnessScheduler 实例（用于重试时调用 Agent）
            model_name: LLM 模型名

        Returns:
            更新后的 {agent_name: report_dict} 字典
        """
        if not reports:
            return reports

        logger.info(
            f"[QualityGate] 开始质量评估 | 阈值={self.min_score} | "
            f"待评估={len(reports)}个"
        )

        quality_scores: Dict[str, float] = {}
        retry_counts: Dict[str, int] = {}
        updated_reports = dict(reports)  # 不修改原字典

        for agent_name, report in updated_reports.items():
            score = score_report(report)
            quality_scores[agent_name] = score

            if score >= self.min_score:
                logger.info(
                    f"[QualityGate] ✅ {agent_name}: {score:.0f}分 → 通过"
                )
                continue

            # 触发重试
            retried = retry_counts.get(agent_name, 0)
            if retried >= self.max_retry:
                logger.warning(
                    f"[QualityGate] ⚠️ {agent_name}: {score:.0f}分 → "
                    f"已达最大重试次数({self.max_retry})，保留原报告"
                )
                continue

            if scheduler is None:
                logger.warning(
                    f"[QualityGate] ⚠️ {agent_name}: {score:.0f}分 → "
                    f"低分但未提供 scheduler，无法重试"
                )
                continue

            # 通过 scheduler 重试该 Agent
            logger.warning(
                f"[QualityGate] 🔄 {agent_name}: {score:.0f}分 → "
                f"触发重试 ({retried + 1}/{self.max_retry})"
            )

            try:
                retry_results = scheduler.run_analysis_agents(
                    stock_code=stock_code,
                    all_data=all_data,
                    selected_agents=[agent_name],
                    model_name=model_name,
                )
                new_report = retry_results.get(agent_name)
                if new_report is None:
                    logger.error(f"[QualityGate] {agent_name} 重试未返回结果")
                    retry_counts[agent_name] = retried + 1
                    continue

                new_score = score_report(new_report)
                quality_scores[agent_name] = new_score

                if new_score > score:
                    updated_reports[agent_name] = new_report
                    logger.info(
                        f"[QualityGate] ✅ {agent_name}: 重试后 {new_score:.0f}分 "
                        f"(+{new_score - score:.0f})"
                    )
                else:
                    logger.info(
                        f"[QualityGate] ⚠️ {agent_name}: 重试后 {new_score:.0f}分, "
                        f"未改善，保留原报告"
                    )
            except Exception as e:
                logger.error(f"[QualityGate] {agent_name} 重试异常: {e}")

            retry_counts[agent_name] = retried + 1

        # 统计
        valid_scores = [s for s in quality_scores.values() if s >= 0]
        avg_score = sum(valid_scores) / max(1, len(valid_scores))
        passed = sum(1 for s in quality_scores.values() if s >= self.min_score)
        total_retries = sum(retry_counts.values())

        logger.info(
            f"[QualityGate] 质量评估完成 | 均分: {avg_score:.0f} | "
            f"通过: {passed}/{len(quality_scores)} | 重试: {total_retries}次"
        )

        return updated_reports


# 默认单例
quality_gate = QualityGate()
