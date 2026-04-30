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
logger = logging.getLogger("首席审核员")

from config.env_config import env_config
from openai import OpenAI
from agents.data_fetcher import DataFetcherAgent
from agents.capital_analyzer import CapitalAnalyzerAgent
from agents.fund_analyzer import FundAnalyzerAgent
from agents.industry_analyzer import IndustryAnalyzerAgent
from agents.risk_analyzer import RiskAnalyzerAgent
from agents.tech_analyzer import TechAnalyzerAgent
from agents.valuation_analyzer import ValuationAnalyzerAgent

# 常量
DEFAULT_MODEL = "qwen"
REPORT_MAX_TOKENS = 2000
LLM_TEMPERATURE = 0.5

# 统一模型配置
MODEL_CONFIG_MAP = {
    "qwen": {"api_key": env_config.QWEN_API_KEY, "base_url": env_config.QWEN_BASE_URL, "model_id": "qwen-plus"},
    "deepseek": {"api_key": env_config.DEEPSEEK_API_KEY, "base_url": env_config.DEEPSEEK_BASE_URL, "model_id": "deepseek-chat"},
    "ernie": {"api_key": env_config.ERNIE_API_KEY, "base_url": env_config.ERNIE_BASE_URL, "model_id": "ernie-4.0"},
    "spark": {"api_key": env_config.SPARK_API_KEY, "base_url": env_config.SPARK_BASE_URL, "model_id": "Spark-4.0"},
    "hunyuan": {"api_key": env_config.HUNYUAN_API_KEY, "base_url": env_config.HUNYUAN_BASE_URL, "model_id": "hunyuan-pro"},
    "doubao": {"api_key": env_config.DOUBAO_API_KEY, "base_url": env_config.DOUBAO_BASE_URL, "model_id": "doubao-pro"}
}

class ChiefReviewerAgent:
    """首席审核员 - 整合所有分析报告并生成最终投资建议"""
    
    def __init__(self, model_name: str = DEFAULT_MODEL):
        self.model_name = model_name.lower()
        self.client = self._init_llm_client()
        
        # 初始化数据获取器
        self.data_fetcher: Optional[DataFetcherAgent] = None
        
        # 初始化各分析Agent，传入同一data_fetcher避免重复采集
        self.capital_agent = CapitalAnalyzerAgent(model_name, data_fetcher=None)
        self.fund_agent = FundAnalyzerAgent(model_name, data_fetcher=None)
        self.industry_agent = IndustryAnalyzerAgent(model_name, data_fetcher=None)
        self.risk_agent = RiskAnalyzerAgent(model_name, data_fetcher=None)
        self.tech_agent = TechAnalyzerAgent(model_name, data_fetcher=None)
        self.valuation_agent = ValuationAnalyzerAgent()

    def _init_llm_client(self) -> OpenAI:
        """初始化大模型客户端"""
        if self.model_name not in MODEL_CONFIG_MAP:
            raise ValueError(f"❌ 暂不支持的模型：{self.model_name}")
        
        config = MODEL_CONFIG_MAP[self.model_name]
        if not config["api_key"]:
            raise ValueError(f"❌ {self.model_name}模型的API Key未配置！")
        
        return OpenAI(api_key=config["api_key"], base_url=config["base_url"])

    def _get_model_id(self) -> str:
        """获取模型ID"""
        return MODEL_CONFIG_MAP[self.model_name]["model_id"]

    def analyze(self, stock_code: str) -> Dict:
        """
        执行完整分析流程
        返回：包含所有维度分析报告和最终整合报告的字典
        """
        logger.info(f"开始对 {stock_code} 进行完整投研分析")
        
        # 1. 初始化数据获取器并获取所有数据（只采集一次）
        self.data_fetcher = DataFetcherAgent(stock_code)
        all_data = self.data_fetcher.get_all_data()
        
        # 给所有Agent设置同一data_fetcher
        self.capital_agent.data_fetcher = self.data_fetcher
        self.fund_agent.data_fetcher = self.data_fetcher
        self.industry_agent.data_fetcher = self.data_fetcher
        self.risk_agent.data_fetcher = self.data_fetcher
        self.tech_agent.data_fetcher = self.data_fetcher
        
        logger.info("数据采集完成，开始各维度分析")
        
        # 2. 并行获取各维度分析报告
        capital_report = self.capital_agent.analyze_capital(stock_code, all_data["capital_data"])
        fund_report = self.fund_agent.analyze_fund(stock_code, all_data["fundamental_data"])
        industry_report = self.industry_agent.analyze_industry(stock_code, all_data["basic_info"])
        risk_report = self.risk_agent.analyze_risk(stock_code, all_data["fundamental_data"])
        tech_report = self.tech_agent.analyze_tech(stock_code, all_data["tech_data"])
        valuation_report = self.valuation_agent.analyze(stock_code)
        
        logger.info("各维度分析完成，开始生成最终整合报告")
        
        # 3. 生成最终整合报告
        final_report = self._synthesize_reports(
            stock_code,
            capital_report,
            fund_report,
            industry_report,
            risk_report,
            tech_report,
            valuation_report,
            all_data
        )
        
        # 4. 返回完整结果
        result = {
            "stock_code": stock_code,
            "basic_info": all_data["basic_info"],
            "reports": {
                "capital": capital_report,
                "fundamental": fund_report,
                "industry": industry_report,
                "risk": risk_report,
                "technical": tech_report,
                "valuation": valuation_report
            },
            "final_report": final_report
        }
        
        logger.info(f"{stock_code} 完整投研分析完成")
        return result

    def _synthesize_reports(
        self,
        stock_code: str,
        capital_report: str,
        fund_report: str,
        industry_report: str,
        risk_report: str,
        tech_report: str,
        valuation_report: str,
        all_data: Dict
    ) -> str:
        """整合各维度报告并生成最终投资建议"""
        
        prompt = f"""
# A股投研整合报告 - 投资总监/首席审核员视角
你是头部公募基金投研总监、拥有20年A股机构投决经验，具备全维度研究整合、独立研判、决策落地的顶级专业能力，需基于团队各分析师提交的完整报告，为{stock_code}生成一份机构级、可直接用于投决会的最终投资研究报告。

## 公司基本信息
{all_data["basic_info"]}

## 团队各维度完整分析报告
### 1. 资金面分析（完整报告）
{capital_report}

### 2. 基本面分析（完整报告）
{fund_report}

### 3. 行业面分析（完整报告）
{industry_report}

### 4. 风险面分析（完整报告）
{risk_report}

### 5. 技术面分析（完整报告）
{tech_report}

### 6. 估值分析（完整报告）
{valuation_report}

## 整合报告要求
请严格基于上述**完整分析报告**进行整合研判，禁止脱离原文数据，报告需覆盖以下核心部分，所有结论必须绑定量化数据：

1. 投资摘要（200字以内）：核心结论、12个月目标价、投资评级、核心驱动因子、风险收益比；
2. 多维度完整报告复盘：原样整合各分析师的完整核心结论，完整呈现资金面、基本面、行业面、风险面、技术面、估值分析的全部分析结果；
3. 综合投资逻辑：基于上述完整报告深度整合，分核心看多逻辑、核心看空逻辑、核心矛盾点三部分，每部分均需引用报告原文数据作为支撑，提炼股价核心驱动与压制因子；
4. 关键风险提示：按优先级列出3-5个核心风险，标注发生概率、股价潜在影响幅度、应对策略；
5. 投资建议：
   - 投资评级（买入/增持/持有/减持/卖出）：附3个以上量化数据支撑；
   - 目标价区间：明确估值测算依据；
   - 建议仓位：分保守/中性/激进给出区间；
   - 入场时机：绑定量化信号；
   - 止损止盈：量化点位+触发条件。

## 输出格式要求
- 语言风格：顶级机构投决会报告标准，专业严谨、逻辑闭环、数据可追溯；
- 数据标注：核心量化指标统一用【】标注，保留2位小数；
- 字数控制：1800-5000字以内；
- 纯文本输出，无需markdown格式，分章节清晰呈现；
- 核心规则：综合投资逻辑必须完全基于团队完整分析报告推导，体现首席投研的整合研判能力。
"""
        
        try:
            completion = self.client.chat.completions.create(
                model=self._get_model_id(),
                messages=[{"role": "user", "content": prompt}],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            report = completion.choices[0].message.content.strip()
            logger.info("最终整合报告生成完成")
            return report
        except Exception as e:
            error_msg = f"生成最终整合报告失败：{str(e)}"
            logger.error(error_msg)
            return f"⚠️ {error_msg}"


# 供LangGraph调用的节点函数
def chief_reviewer_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    # 从 state 中获取各维度报告（如果有）
    capital_report = state.get("capital_report", None)
    fund_report = state.get("fund_report", None)
    industry_report = state.get("industry_report", None)
    risk_report = state.get("risk_report", None)
    tech_report = state.get("tech_report", None)
    valuation_report = state.get("valuation_report", None)
    
    # 如果 state 中已有各维度报告，直接整合；否则完整运行
    if capital_report and fund_report and industry_report and risk_report and tech_report and valuation_report:
        reviewer = ChiefReviewerAgent(DEFAULT_MODEL)
        # 这里可以添加一个直接整合已有报告的方法
        # 暂时先完整运行
        result = reviewer.analyze(stock_code)
    else:
        reviewer = ChiefReviewerAgent(DEFAULT_MODEL)
        result = reviewer.analyze(stock_code)
    
    state["final_report"] = result["final_report"]
    state["reports"] = result["reports"]
    return state

if __name__ == "__main__":
    try:
        reviewer = ChiefReviewerAgent(DEFAULT_MODEL)
        result = reviewer.analyze("600519")
        
        print("\n" + "="*80)
        print("【600519 完整投研报告】")
        print("="*80)
        print("\n--- 最终整合报告 ---")
        print(result["final_report"])
        
        print("\n--- 各维度报告 ---")
        print("\n【资金面】")
        print(result["reports"]["capital"])
        print("\n【基本面】")
        print(result["reports"]["fundamental"])
        print("\n【行业面】")
        print(result["reports"]["industry"])
        print("\n【风险面】")
        print(result["reports"]["risk"])
        print("\n【技术面】")
        print(result["reports"]["technical"])
        print("\n【估值面】")
        print(result["reports"]["valuation"])
        
    except Exception as e:
        logger.error(f"执行失败：{str(e)}")
        print(f"\n❌ 执行失败：{str(e)}")
