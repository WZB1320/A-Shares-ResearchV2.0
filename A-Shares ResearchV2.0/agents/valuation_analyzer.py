import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from langchain_core.messages import HumanMessage
from config.llm_config import llm
from agents.data_fetcher import DataFetcherAgent
import logging

logger = logging.getLogger("估值分析Agent")

class ValuationAnalyzerAgent:
    """
    估值分析Agent（独立模块）
    功能：计算个股PE/PB近10年分位、历史均值、行业对比，给出专业估值判断
    输出：估值水平（低估/合理/高估）+ 完整估值分析报告
    """

    def __init__(self):
        self.llm = llm

    def calculate_valuation_percentile(self, current_value: float, historical_data: list) -> float:
        """
        核心工具：计算当前值在历史数据中的百分位（0~100）
        分位越低 → 越便宜
        分位越高 → 越贵
        """
        if not historical_data or current_value <= 0:
            return 0.0

        sorted_data = sorted([x for x in historical_data if x > 0])
        count = len([x for x in sorted_data if x <= current_value])
        percentile = round((count / len(sorted_data)) * 100, 2)
        return percentile

    def get_valuation_level(self, pe_percentile: float, pb_percentile: float) -> str:
        """
        根据PE、PB分位判断估值水平
        """
        score = (pe_percentile + pb_percentile) / 2

        if score <= 20:
            return "严重低估"
        elif score <= 40:
            return "低估"
        elif score <= 60:
            return "合理估值"
        elif score <= 80:
            return "高估"
        else:
            return "严重高估"

    def analyze(self, stock_code: str) -> str:
        """
        入口函数：生成估值分析报告
        """
        try:
            # 1. 从统一数据获取Agent拿全量数据（不重复拉取，架构统一）
            data_fetcher = DataFetcherAgent(stock_code)
            all_data = data_fetcher.get_all_data()

            basic_info = all_data["basic_info"]
            fundamental_data = all_data["fundamental_data"]
            valuation_data = fundamental_data.get("valuation", {})

            # 2. 提取核心估值指标
            pe_ttm = valuation_data.get("PE-TTM", 0)
            pb_lf = valuation_data.get("PB-LF", 0)
            pe_history = fundamental_data.get("pe_history", [])
            pb_history = fundamental_data.get("pb_history", [])

            # 3. 计算10年分位
            pe_percentile = self.calculate_valuation_percentile(pe_ttm, pe_history)
            pb_percentile = self.calculate_valuation_percentile(pb_lf, pb_history)

            # 4. 综合估值水平
            valuation_level = self.get_valuation_level(pe_percentile, pb_percentile)

            # 5. 行业对比（简化）
            industry = basic_info.get("行业", "未知行业")

            # 6. 构建专业Prompt
            prompt = f"""
你是资深A股估值分析师，为股票{stock_code}生成专业、严谨、机构级估值分析报告。

【基础估值数据】
- 当前PE-TTM：{pe_ttm}
- 当前PB-LF：{pb_lf}
- PE近10年分位：{pe_percentile}%
- PB近10年分位：{pb_percentile}%
- 综合估值水平：{valuation_level}
- 所属行业：{industry}

【分析要求】
1. 说明当前估值在历史上的位置（便宜/中等/贵）
2. 解释PE、PB分位的含义
3. 判断估值是否具备安全边际
4. 给出估值层面的结论（是否具备投资价值）
5. 语言专业、简洁、量化，800字以内

输出纯文本，无需格式。
"""

            # 7. 调用大模型生成报告
            response = self.llm.invoke([HumanMessage(content=prompt)])
            report = response.content.strip()
            logger.info(f"{stock_code} 估值分析完成 | 估值水平：{valuation_level}")
            return report

        except Exception as e:
            error_msg = f"估值分析异常：{str(e)}"
            logger.error(error_msg)
            return error_msg

# 供LangGraph调用的节点函数
def valuation_analyzer_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = ValuationAnalyzerAgent()
    valuation_report = agent.analyze(stock_code)
    state["valuation_report"] = valuation_report
    return state