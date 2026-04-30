import sys
import logging
from pathlib import Path
from typing import Dict, Optional

sys.path.append(str(Path(__file__).parent.parent))

# agents/risk_analyzer.py
"""风险面分析Agent - 量化全维度风险，给出风控策略（机构级）"""
from config.env_config import env_config
from openai import OpenAI
from agents.data_fetcher import DataFetcherAgent

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] %(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("风险面Agent")

# 常量
DEFAULT_MODEL = "qwen"
REPORT_MAX_TOKENS = 800
LLM_TEMPERATURE = 0.5

# 统一模型配置
MODEL_CONFIG_MAP = {
    "qwen": {"api_key": env_config.QWEN_API_KEY, "base_url": env_config.QWEN_BASE_URL, "model_id": "qwen-plus"},
    "deepseek": {"api_key": env_config.DEEPSEEK_API_KEY, "base_url": env_config.DEEPSEEK_BASE_URL, "model_id": "deepseek-chat"},
    "ernie": {"api_key": env_config.ERNIE_API_KEY, "base_url": env_config.ERNIE_BASE_URL, "model_id": "ernie-4.0"},
    "doubao": {"api_key": env_config.DOUBAO_API_KEY, "base_url": env_config.DOUBAO_BASE_URL, "model_id": "doubao-pro"},
    "spark": {"api_key": env_config.SPARK_API_KEY, "base_url": env_config.SPARK_BASE_URL, "model_id": "Spark-4.0"},
    "hunyuan": {"api_key": env_config.HUNYUAN_API_KEY, "base_url": env_config.HUNYUAN_BASE_URL, "model_id": "hunyuan-pro"}
}

class RiskAnalyzerAgent:
    """风险面分析智能体：量化估值/财务/市场/黑天鹅风险，输出专业风控策略"""
    def __init__(self, model_name: str = "qwen", data_fetcher: Optional[DataFetcherAgent] = None):
        self.model_name = model_name.lower()
        self.model_id = self._get_model_id()
        self.client = self._init_llm_client()
        self.data_fetcher = data_fetcher

    def _get_model_id(self) -> str:
        """统一获取模型ID（与项目所有Agent保持一致）"""
        return MODEL_CONFIG_MAP.get(self.model_name, "qwen-plus")["model_id"]

    def _init_llm_client(self) -> OpenAI:
        """初始化大模型客户端（全模型兼容，配置统一）"""
        config = MODEL_CONFIG_MAP[self.model_name]
        return OpenAI(api_key=config["api_key"], base_url=config["base_url"])

    def analyze_risk(self, stock_code: str, fundamental_data: Optional[Dict] = None) -> str:
        """执行风险面分析，返回机构级量化报告"""
        # 1. 获取风险分析基础数据（优先使用传入的数据，否则自己获取）
        if fundamental_data is None:
            if self.data_fetcher is None:
                self.data_fetcher = DataFetcherAgent(stock_code)
            fundamental_data = self.data_fetcher.get_fundamental_data()
        
        basic_info = fundamental_data.get("basic_info", {})
        valuation = fundamental_data.get("valuation", {})
        
        # 2. 构造专业Prompt
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
        # 3. 标准化LLM调用
        try:
            completion = self.client.chat.completions.create(
                model=self.model_id,
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

# 供LangGraph调用的节点函数
def risk_analyzer_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    fundamental_data = state.get("fundamental_data", None)
    agent = RiskAnalyzerAgent("qwen")
    risk_report = agent.analyze_risk(stock_code, fundamental_data)
    state["risk_report"] = risk_report
    return state

if __name__ == "__main__":
    # 增加异常捕获，运行更稳定
    try:
        agent = RiskAnalyzerAgent("qwen")
        report = agent.analyze_risk("600519")
        print("\n" + "="*60)
        print("【600519 风险面分析报告】")
        print("="*60)
        print(report)
    except Exception as e:
        print(f"\n❌ 分析执行失败：{e}")