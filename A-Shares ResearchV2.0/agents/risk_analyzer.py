import sys
import logging
from pathlib import Path
from typing import Dict, Optional

sys.path.append(str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] %(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("风险面Agent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from agents.data_fetcher import DataFetcherAgent

REPORT_MAX_TOKENS = 800
LLM_TEMPERATURE = 0.5

class RiskAnalyzerAgent:
    """风险面分析智能体：量化估值/财务/市场/黑天鹅风险，输出专业风控策略"""
    def __init__(self, model_name: str = DEFAULT_MODEL, data_fetcher: Optional[DataFetcherAgent] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_fetcher = data_fetcher

    def analyze_risk(self, stock_code: str, fundamental_data: Optional[Dict] = None) -> str:
        """执行风险面分析，返回机构级量化报告"""
        if fundamental_data is None:
            if self.data_fetcher is None:
                self.data_fetcher = DataFetcherAgent(stock_code)
            fundamental_data = self.data_fetcher.get_fundamental_data()
        
        basic_info = fundamental_data.get("basic_info", {})
        valuation = fundamental_data.get("valuation", {})
        
        prompt = f"""
# A股风险面量化分析（机构级）
分析{stock_code}全维度风险，输出风险评分0-100，带【数据】
1. 估值风险：PE/PB历史分位、高估概率
2. 财务风险：资产负债率、现金流
3. 市场风险：波动率、流动性
4. 黑天鹅风险：商誉、质押
5. 风控建议：止损线、仓位上限、减仓条件
纯文本，800字内，专业量化

## 数据参考
- 公司基本信息: {basic_info}
- 估值数据: {valuation}
"""
        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[{"role": "user", "content": prompt}],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            report = completion.choices[0].message.content.strip()
            logger.info(f"{stock_code}风险面分析报告生成完成")
            return report
        except Exception as e:
            error_msg = f"生成{stock_code}风险面报告失败：{str(e)}"
            logger.error(error_msg)
            return f"⚠️ {error_msg}"

def risk_analyzer_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    fundamental_data = state.get("fundamental_data", None)
    agent = RiskAnalyzerAgent(DEFAULT_MODEL)
    risk_report = agent.analyze_risk(stock_code, fundamental_data)
    state["risk_report"] = risk_report
    return state

if __name__ == "__main__":
    try:
        agent = RiskAnalyzerAgent(DEFAULT_MODEL)
        report = agent.analyze_risk("600519")
        print("\n" + "="*60)
        print("【600519 风险面分析报告】")
        print("="*60)
        print(report)
    except Exception as e:
        print(f"\n❌ 分析执行失败：{e}")
