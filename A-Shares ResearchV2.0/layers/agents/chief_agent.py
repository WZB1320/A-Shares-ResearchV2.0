import sys
import logging
import time
from pathlib import Path
from typing import Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(str(Path(__file__).parent.parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] %(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("ChiefAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.agents.tech_agent import TechAgent
from layers.agents.fund_agent import FundAgent
from layers.agents.capital_agent import CapitalAgent
from layers.agents.industry_agent import IndustryAgent
from layers.agents.risk_agent import RiskAgent
from layers.agents.valuation_agent import ValuationAgent
from layers.skills import (
    TechSkill, FundSkill, CapitalSkill,
    IndustrySkill, RiskSkill, ValuationSkill
)

REPORT_MAX_TOKENS = 3000
LLM_TEMPERATURE = 0.3
MAX_PARALLEL_WORKERS = 6


class ChiefAgent:
    """
    首席Agent - 机构级投研整合（Anthropic Harness标准）
    职责：协调各专业Agent，整合量化信号，生成机构级投决会报告
    优化：6个维度Agent并行执行，减少LLM串行等待时间
    """

    def __init__(self, model_name: str = DEFAULT_MODEL, data_connector: Optional[DataConnector] = None):
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector

    def analyze(self, stock_code: str, selected_agents: list = None) -> Dict:
        start_time = time.time()
        logger.info(f"[ChiefAgent] 开始机构级完整投研分析: {stock_code} | 选中Agent: {selected_agents}")

        # 默认全选
        if selected_agents is None:
            selected_agents = ["tech", "fund", "capital", "industry", "risk", "valuation"]
        
        # 确保是列表
        if isinstance(selected_agents, str):
            selected_agents = [selected_agents]
        
        # 保存选中的agent列表，用于报告生成
        self.selected_agents = selected_agents

        if self.data_connector is None:
            self.data_connector = DataConnector(stock_code)

        all_data = self.data_connector.fetch_all()
        data_fetch_time = time.time() - start_time
        logger.info(f"[ChiefAgent] 数据获取完成，耗时: {data_fetch_time:.2f}秒")

        tech_data = all_data.get("tech_data")
        fundamental_data = all_data.get("fundamental_data")
        financial_data = all_data.get("financial_data")
        capital_data = all_data.get("capital_data")
        valuation_data = all_data.get("valuation_data")

        # 先计算量化信号（本地计算，很快）- 只计算选中的
        skill_start = time.time()
        tech_signals = TechSkill.analyze(tech_data) if tech_data and "tech" in selected_agents else None
        fund_signals = FundSkill.analyze(financial_data) if financial_data and "fund" in selected_agents else None
        capital_signals = CapitalSkill.analyze(capital_data) if capital_data and "capital" in selected_agents else None
        industry_signals = IndustrySkill.analyze(fundamental_data) if fundamental_data and "industry" in selected_agents else None
        risk_signals = RiskSkill.analyze(financial_data, tech_data) if financial_data and "risk" in selected_agents else None
        valuation_signals = ValuationSkill.analyze(valuation_data, fundamental_data) if valuation_data and fundamental_data and "valuation" in selected_agents else None
        logger.info(f"[ChiefAgent] 量化信号计算完成，耗时: {time.time() - skill_start:.2f}秒")

        # 并行执行选中的Agent的LLM分析
        agent_start = time.time()
        logger.info(f"[ChiefAgent] 各维度Agent并行分析开始 | 选中: {selected_agents}")
        
        agents_map = {
            "tech": (TechAgent, tech_data, None),
            "fund": (FundAgent, fundamental_data, None),
            "capital": (CapitalAgent, capital_data, None),
            "industry": (IndustryAgent, fundamental_data, None),
            "risk": (RiskAgent, financial_data, tech_data),
            "valuation": (ValuationAgent, valuation_data, fundamental_data),
        }
        
        reports = {}
        active_agents = {k: v for k, v in agents_map.items() if k in selected_agents}
        
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as executor:
            future_to_agent = {}
            for name, (AgentClass, data1, data2) in active_agents.items():
                agent = AgentClass(self.model_name, self.data_connector)
                if name == "risk":
                    future = executor.submit(agent.analyze, stock_code, data1, data2)
                elif name == "valuation":
                    future = executor.submit(agent.analyze, stock_code, data1, data2)
                else:
                    future = executor.submit(agent.analyze, stock_code, data1)
                future_to_agent[future] = name
            
            for future in as_completed(future_to_agent):
                agent_name = future_to_agent[future]
                try:
                    reports[agent_name] = future.result()
                    logger.info(f"[ChiefAgent] {agent_name}_agent 分析完成")
                except Exception as e:
                    logger.error(f"[ChiefAgent] {agent_name}_agent 分析失败: {e}")
                    reports[agent_name] = f"⚠️ {agent_name}分析失败: {str(e)}"
        
        agent_time = time.time() - agent_start
        logger.info(f"[ChiefAgent] 各维度Agent并行分析完成，耗时: {agent_time:.2f}秒")

        # 解包报告（未选中的返回空字符串）
        tech_report = reports.get("tech", "【未选择技术面分析】")
        fund_report = reports.get("fund", "【未选择基本面分析】")
        capital_report = reports.get("capital", "【未选择资金面分析】")
        industry_report = reports.get("industry", "【未选择行业面分析】")
        risk_report = reports.get("risk", "【未选择风险面分析】")
        valuation_report = reports.get("valuation", "【未选择估值面分析】")

        logger.info("[ChiefAgent] 开始整合机构级报告")
        final_report = self.synthesize_reports(
            stock_code,
            capital_report,
            fund_report,
            industry_report,
            risk_report,
            tech_report,
            valuation_report,
            all_data.get("basic_info", {}),
            tech_signals,
            fund_signals,
            capital_signals,
            industry_signals,
            risk_signals,
            valuation_signals
        )

        result = {
            "stock_code": stock_code,
            "basic_info": all_data.get("basic_info", {}),
            "quant_signals": {
                "tech": tech_signals,
                "fund": fund_signals,
                "capital": capital_signals,
                "industry": industry_signals,
                "risk": risk_signals,
                "valuation": valuation_signals
            },
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

        return result

    def synthesize_reports(
        self,
        stock_code: str,
        capital_report: str,
        fund_report: str,
        industry_report: str,
        risk_report: str,
        tech_report: str,
        valuation_report: str,
        basic_info: Dict,
        tech_signals=None,
        fund_signals=None,
        capital_signals=None,
        industry_signals=None,
        risk_signals=None,
        valuation_signals=None
    ) -> str:

        # 确保有选中的agent列表
        if not hasattr(self, 'selected_agents') or self.selected_agents is None:
            self.selected_agents = ["tech", "fund", "capital", "industry", "risk", "valuation"]

        # 构建量化信号摘要（仅包含选中的agent）
        quant_summary = []
        if tech_signals and "tech" in self.selected_agents:
            quant_summary.append(f"技术面：评分{tech_signals.overall_score}/100，趋势{tech_signals.trend_strength.value}，建议{tech_signals.research_advice}")
        if fund_signals and "fund" in self.selected_agents:
            quant_summary.append(f"基本面：评分{fund_signals.overall_score}/100，评级{fund_signals.investment_grade}，建议{fund_signals.research_advice}")
        if capital_signals and "capital" in self.selected_agents:
            quant_summary.append(f"资金面：评分{capital_signals.overall_score}/100，评级{capital_signals.capital_grade}，建议{capital_signals.research_advice}")
        if industry_signals and "industry" in self.selected_agents:
            quant_summary.append(f"行业面：评分{industry_signals.overall_score}/100，评级{industry_signals.industry_grade}，建议{industry_signals.research_advice}")
        if risk_signals and "risk" in self.selected_agents:
            quant_summary.append(f"风险面：评分{risk_signals.overall_risk_score}/100，等级{risk_signals.overall_risk_level.value}，建议{risk_signals.research_advice}")
        if valuation_signals and "valuation" in self.selected_agents:
            quant_summary.append(f"估值面：评分{valuation_signals.overall_score}/100，水平{valuation_signals.valuation_level.value}，建议{valuation_signals.research_advice}")

        quant_text = "\n".join(quant_summary) if quant_summary else "量化信号暂不可用"

        # 构建各维度分析报告（仅包含选中的agent）
        dimension_reports = []
        if "tech" in self.selected_agents and not tech_report.startswith("【未选择"):
            dimension_reports.append(f"### 1. 技术面分析\n{tech_report}")
        if "fund" in self.selected_agents and not fund_report.startswith("【未选择"):
            dimension_reports.append(f"### 2. 基本面分析\n{fund_report}")
        if "capital" in self.selected_agents and not capital_report.startswith("【未选择"):
            dimension_reports.append(f"### 3. 资金面分析\n{capital_report}")
        if "industry" in self.selected_agents and not industry_report.startswith("【未选择"):
            dimension_reports.append(f"### 4. 行业面分析\n{industry_report}")
        if "risk" in self.selected_agents and not risk_report.startswith("【未选择"):
            dimension_reports.append(f"### 5. 风险面分析\n{risk_report}")
        if "valuation" in self.selected_agents and not valuation_report.startswith("【未选择"):
            dimension_reports.append(f"### 6. 估值面分析\n{valuation_report}")

        dimension_text = "\n\n".join(dimension_reports) if dimension_reports else "（无选中的分析维度）"

        # 构建选中维度列表，用于告诉LLM哪些维度要分析
        selected_dimensions = []
        if "tech" in self.selected_agents:
            selected_dimensions.append("- 技术面趋势强度与关键价位")
        if "fund" in self.selected_agents:
            selected_dimensions.append("- 基本面杜邦分析与盈利质量")
        if "capital" in self.selected_agents:
            selected_dimensions.append("- 资金面共识度与机构行为")
        if "industry" in self.selected_agents:
            selected_dimensions.append("- 行业面景气度与竞争格局")
        if "risk" in self.selected_agents:
            selected_dimensions.append("- 风险面综合评估与缓释措施")
        if "valuation" in self.selected_agents:
            selected_dimensions.append("- 估值面安全边际与DCF合理性")
        
        selected_dim_text = "\n".join(selected_dimensions) if selected_dimensions else "（请选择至少一个分析维度）"

        prompt = f"""
# A股机构级投研整合报告 - 投决会标准
你是头部公募基金投研总监，拥有20年以上A股机构投决经验，需为{stock_code}生成一份投决会级别研究报告。

## 公司基本信息
{basic_info}

## 六维量化信号摘要
{quant_text}

## 各维度机构级分析报告
{dimension_text}

## 整合报告要求（投决会标准）
1. 投资摘要（300字以内）：
   - 核心结论与综合评级（买入/增持/持有/减持/卖出）
   - 目标价区间与估值依据
   - 核心驱动因素与关键假设
   - 预期收益率与风险收益比

2. 六维量化复盘（仅针对以下已选中的维度进行分析）：
{selected_dim_text}

3. 综合投资逻辑：
   - 看多逻辑（3-5条，每条附数据支撑）
   - 看空逻辑（2-3条，每条附数据支撑）
   - 核心矛盾点与情景分析
   - 催化剂与风险触发点

4. 机构级风险提示：
   - 3-5个核心风险因子
   - 风险发生概率评估（高/中/低）
   - 潜在影响幅度测算
   - 风险缓释措施与应对策略

5. 机构级投资建议：
   - 投资评级（买入/增持/持有/减持/卖出）+ 3个量化支撑
   - 目标价区间（基准/乐观/悲观情景）
   - 估值依据（PE/PB/DCF）
   - 建议仓位（保守3%/中性5%/激进8%）
   - 入场时机（立即/回调/突破）
   - 止损位（-8%/-10%/-15%）与止盈位（+20%/+30%/+50%）
   - 持有周期（短期1-3月/中期3-6月/长期6-12月）

## 输出要求
- 核心数据用【】标注
- 所有结论必须有量化数据支撑
- 拒绝模糊表述，必须明确评级、目标价、仓位
- 只针对已选中的维度进行分析，未选择的维度不要提及
- 总字数2000-3000字，纯文本输出
"""
        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[{"role": "user", "content": prompt}],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            report = completion.choices[0].message.content.strip()
            logger.info(f"[ChiefAgent] 机构级整合报告生成完成: {stock_code}")
            return report
        except Exception as e:
            error_msg = f"生成{stock_code}整合报告失败：{str(e)}"
            logger.error(f"[ChiefAgent] {error_msg}")
            return f"⚠️ {error_msg}"


def chief_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = ChiefAgent(model_name=DEFAULT_MODEL)
    result = agent.analyze(stock_code)
    state["final_report"] = result["final_report"]
    state["reports"] = result["reports"]
    state["quant_signals"] = result.get("quant_signals", {})
    return state


chief_agent = ChiefAgent
