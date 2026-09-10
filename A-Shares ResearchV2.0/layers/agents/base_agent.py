"""Base Analysis Agent - 提取所有分析 Agent 的公共逻辑

所有分析 Agent 共享相同的生命周期：
1. __init__: 初始化 LLM 客户端和数据连接器
2. analyze: 入口方法 → 获取数据 → 运行 Skill → 调用 LLM → 返回报告
3. _get_data: 从 state 或 data_connector 获取原始数据
4. _run_skill: Skill 计算得到结构化信号
5. _build_prompt: 构建 prompt 传给 LLM
6. _call_llm: LLM 调用 + JSON 解析
"""

import logging
from typing import Dict, Optional
from abc import ABC, abstractmethod

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL, chat_completion
from layers.connectors import DataConnector
from layers.agents.report_schema import (
    parse_json_report, error_report, unavailable_report,
    grade_score_gap, _score_to_grade,
)
from layers.validators.hallucination_validator import (
    validate_report_numbers, build_correction_feedback,
)

logger = logging.getLogger("BaseAnalysisAgent")


# 量化锚定强制规则 —— 所有分析 Agent 的 prompt 通用规则
# 目的：消除"极低/极高/大幅"等模糊描述，强制所有判断附带具体数字
QUANT_ANCHOR_RULE = """
**量化锚定强制规则**：
- 所有判断必须附带具体数字，禁止使用"极低/极高/很大/很小/大幅/显著"等模糊描述
- ✅ 正确示例："换手率0.3%低于历史80%分位"、"融资余额下降9.85%"、"ROE为-4.62%"
- ❌ 错误示例："换手率极低"、"融资余额大幅下降"、"ROE较差"
- key_signals 和 risk_factors 中的每一项必须包含至少1个具体数字（百分比/倍数/元/亿元等）
- 数字来源限于提供的数据上下文，不得编造未提供的数据

**金额单位换算规则**（换算错误是最严重的数字错误，一律按此执行）：
- 亿 = 元 ÷ 1e8（8个零），万 = 元 ÷ 1e4（4个零）。例：563939375元 = 5.64亿元（不是56.39亿）
- 优先直接使用数据上下文已呈现的书写口径；确需换算时，先数清原值的数量级再换算

**报告期口径规则**：
- 引用财务指标（ROE/毛利率/增速/负债率等）时必须标注报告期，如"2026Q2（截至2026-06-30）ROE为-2.06%"
- 数据上下文标注了"财务数据报告期"时，描述当前状态必须使用该最新报告期数据；历史报告期数据仅可用于趋势对比，且必须注明所属期间，禁止把历史报告期数据当作当前状态引用

**评分-评级一致性规则**：
- overall_score 与 grade 必须按标准映射对齐：≥90强烈看多 / 70-89看多 / 58-69中性偏多 / 42-57中性 / 30-41中性偏空 / 15-29看空 / <15强烈看空
- 禁止评分与评级脱节（如评分45却给"看空"）。若因数据不足等因素想表达额外谨慎，应下调 overall_score，而不是单独压低 grade"""


class BaseAnalysisAgent(ABC):
    """所有分析 Agent 的抽象基类"""

    DEFAULT_MAX_TOKENS = 1200
    DEFAULT_TEMPERATURE = 0.0

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.max_tokens = getattr(self, "MAX_TOKENS", self.DEFAULT_MAX_TOKENS)
        self.temperature = getattr(self, "LLM_TEMPERATURE", self.DEFAULT_TEMPERATURE)

    def analyze(self, stock_code: str, state: Optional[Dict] = None) -> Dict:
        """标准分析流程：获取数据 → Skill计算 → LLM分析 → 返回结果"""
        logger.info(f"[{self.__class__.__name__}] 开始分析: {stock_code}")

        try:
            data = self._get_data(state)
        except Exception as e:
            error_msg = f"数据获取失败：{str(e)}"
            logger.error(f"[{self.__class__.__name__}] {error_msg}")
            return error_report(self.dimension, error_msg).to_dict()

        if data is None:
            logger.warning(f"[{self.__class__.__name__}] 数据缺失: {stock_code}")
            return unavailable_report(self.dimension).to_dict()

        try:
            signals = self._run_skill(data)
        except Exception as e:
            error_msg = f"{self.dimension}指标计算失败：{str(e)}"
            logger.error(f"[{self.__class__.__name__}] {error_msg}")
            return error_report(self.dimension, error_msg).to_dict()

        quality_context = state.get("quality_context") if state else None

        prompt = self._build_prompt(stock_code, signals, quality_context)

        # 数字幻觉校验上下文：prompt 中数据上下文 + quality_context（含知识库历史跟踪/共识，
        # 其中历史数字可被 LLM 合法引用，必须纳入校验池，否则误判为幻觉）
        try:
            verification_ctx = self._verification_context(stock_code, signals)
            if quality_context:
                verification_ctx = f"{verification_ctx}\n\n{quality_context}"
        except Exception as e:
            logger.warning(f"[{self.__class__.__name__}] 校验上下文构建失败，跳过幻觉校验: {e}")
            verification_ctx = None

        try:
            report = None
            check = None
            for attempt in range(2):  # 幻觉率超阈值时修正重试一次
                raw = chat_completion(
                    model_name=self.model_name,
                    messages=[
                        {"role": "system", "content": self._system_prompt()},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=self.temperature,
                    max_tokens=self.max_tokens,
                    task_tag=self.__class__.__name__,
                )
                report = parse_json_report(raw, self.dimension)
                if verification_ctx is None or not report.is_valid():
                    break
                check = validate_report_numbers(report.to_dict(), verification_ctx)
                if check.passed:
                    break
                logger.warning(
                    f"[{self.__class__.__name__}] 数字幻觉率 {check.hallucination_rate:.0%} "
                    f"超阈值（{check.threshold:.0%}），第{attempt + 1}次修正重试: {stock_code}"
                )
                prompt = prompt + build_correction_feedback(check)

            result = report.to_dict()

            # 评分-评级一致性兜底：脱节 ≥2 档时按标准映射纠正 grade
            # （评分是量化锚点，grade 标签以评分为准；LLM 的额外谨慎应体现在 score 上）
            if report.is_valid():
                gap = grade_score_gap(report.overall_score, report.grade)
                if gap is not None and gap >= 2:
                    corrected = _score_to_grade(report.overall_score)
                    logger.warning(
                        f"[{self.__class__.__name__}] 评分-评级脱节（gap={gap}档），"
                        f"grade 由 {report.grade} 纠正为 {corrected}"
                        f"（score={report.overall_score}）: {stock_code}"
                    )
                    report.grade = corrected
                    report.grade_corrected = True
                    result = report.to_dict()

            if check is not None:
                result["data_verification"] = check.to_dict()
                if not check.passed:
                    logger.error(
                        f"[{self.__class__.__name__}] 修正重试后幻觉率仍为 "
                        f"{check.hallucination_rate:.0%}，报告已标记: {stock_code}"
                    )
            logger.info(f"[{self.__class__.__name__}] 分析完成: {stock_code} | score={report.overall_score} grade={report.grade}")
            return result
        except Exception as e:
            error_msg = f"LLM调用失败：{str(e)}"
            logger.error(f"[{self.__class__.__name__}] {error_msg}")
            return error_report(self.dimension, error_msg).to_dict()

    @staticmethod
    def _fmt(value, suffix="", precision=2):
        """格式化数值，0 或 None 标记为数据缺失"""
        if value is None or value == 0:
            return "数据缺失"
        fmt_str = f"{{:.{precision}f}}"
        return f"{fmt_str.format(value)}{suffix}"

    @abstractmethod
    def _get_data(self, state: Optional[Dict]):
        """从 state 或 data_connector 获取原始数据"""
        pass

    @abstractmethod
    def _run_skill(self, data):
        """调用 Skill 计算得到结构化信号"""
        pass

    @abstractmethod
    def _build_prompt(self, stock_code: str, signals, quality_context: Optional[str]) -> str:
        """构建 LLM prompt"""
        pass

    def _verification_context(self, stock_code: str, signals) -> str:
        """数字幻觉校验用的数据上下文。

        默认与 prompt 中的数据上下文一致（各 Agent 的 _build_data_context）。
        若 Agent 的 prompt 中还拼接了其他附加数据（如 ValuationAgent 的基本面摘要），
        需覆盖此方法返回完整的数据上下文，否则附加数据中的数字会被误判为幻觉。
        """
        return self._build_data_context(stock_code, signals)

    @abstractmethod
    def _system_prompt(self) -> str:
        """系统提示词"""
        pass

    @property
    @abstractmethod
    def dimension(self) -> str:
        """维度名称: tech/fund/capital/risk/industry/valuation"""
        pass
