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

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.agents.report_schema import parse_json_report, error_report, unavailable_report

logger = logging.getLogger("BaseAnalysisAgent")


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

        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[
                    {"role": "system", "content": self._system_prompt()},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )
            raw = completion.choices[0].message.content.strip()
            report = parse_json_report(raw, self.dimension)
            logger.info(f"[{self.__class__.__name__}] 分析完成: {stock_code} | score={report.overall_score} grade={report.grade}")
            return report.to_dict()
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

    @abstractmethod
    def _system_prompt(self) -> str:
        """系统提示词"""
        pass

    @property
    @abstractmethod
    def dimension(self) -> str:
        """维度名称: tech/fund/capital/risk/industry/valuation"""
        pass
