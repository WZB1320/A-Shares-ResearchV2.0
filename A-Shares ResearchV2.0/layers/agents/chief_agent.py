import sys
import logging
import time
from pathlib import Path
from typing import Dict, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("ChiefAgent")

from config.llm_config import get_llm_client, get_model_id, DEFAULT_MODEL
from layers.connectors import DataConnector
from layers.agents.tech_agent import TechAgent
from layers.agents.fund_agent import FundAgent
from layers.agents.capital_agent import CapitalAgent
from layers.agents.industry_agent import IndustryAgent
from layers.agents.risk_agent import RiskAgent
from layers.agents.valuation_agent import ValuationAgent
from layers.agents.report_schema import (
    AgentReport, aggregate_reports, reports_to_markdown,
    error_report, unavailable_report
)
from layers.agents.debate import (
    run_debate_rounds, build_debate_summary, CROSS_PAIRINGS, DIM_LABELS as DEBATE_LABELS
)
from layers.validators import validator as data_validator
from layers.memory.knowledge_base import (
    save_snapshot, build_tracking_context, get_report_consensus,
)

REPORT_MAX_TOKENS = 3000
LLM_TEMPERATURE = 0.0
MAX_PARALLEL_WORKERS = 6


class ChiefAgent:

    def __init__(
        self,
        model_name: str = DEFAULT_MODEL,
        data_connector: Optional[DataConnector] = None,
        use_scheduler: bool = False,
    ):
        """
        Args:
            model_name: LLM 模型名
            data_connector: 数据连接器（可选，未提供则内部创建）
            use_scheduler: 是否使用 HarnessScheduler 编排分析阶段
                - False（默认）: 使用原 ThreadPoolExecutor 并行块（兼容模式）
                - True: 使用 HarnessScheduler，带状态管理/重试/检查点
                验证通过后默认值将改为 True
        """
        self.model_name = model_name.lower()
        self.client = get_llm_client(self.model_name)
        self.data_connector = data_connector
        self.use_scheduler = use_scheduler
        self.selected_agents = None
        self.scheduler = self._build_scheduler() if use_scheduler else None
        if use_scheduler:
            logger.info("[ChiefAgent] 启用 HarnessScheduler 编排模式")

    def _build_scheduler(self):
        """构建并注册所有分析 Agent 到 HarnessScheduler"""
        from harness.scheduler import HarnessScheduler, SchedulerConfig

        scheduler = HarnessScheduler(SchedulerConfig(
            max_retries=2,
            parallel_execution=True,
            max_workers=MAX_PARALLEL_WORKERS,
            checkpoint_enabled=True,
            validation_enabled=False,  # ChiefAgent 已有自己的数据校验
        ))

        # 注册各 Agent + 数据依赖构建器
        scheduler.register_agent(
            "tech", TechAgent,
            data_payload_builder=lambda d: {"tech_data": d.get("tech_data")},
        )
        scheduler.register_agent(
            "fund", FundAgent,
            data_payload_builder=lambda d: {"fundamental_data": d.get("fundamental_data")},
        )
        scheduler.register_agent(
            "capital", CapitalAgent,
            data_payload_builder=lambda d: {"capital_data": d.get("capital_data")},
        )
        scheduler.register_agent(
            "industry", IndustryAgent,
            data_payload_builder=lambda d: {"fundamental_data": d.get("fundamental_data")},
        )
        scheduler.register_agent(
            "risk", RiskAgent,
            data_payload_builder=lambda d: {
                "financial_data": d.get("financial_data") or d.get("fundamental_data"),
                "tech_data": d.get("tech_data"),
            },
        )
        scheduler.register_agent(
            "valuation", ValuationAgent,
            data_payload_builder=lambda d: {
                "valuation_data": d.get("valuation_data"),
                "fundamental_data": d.get("fundamental_data"),
            },
        )
        return scheduler

    def analyze(self, stock_code: str, state: Optional[Dict] = None) -> str:
        start_time = time.time()

        selected_agents = None
        if state is not None:
            selected_agents = state.get("selected_agents")

        if selected_agents is None:
            selected_agents = ["tech", "fund", "capital", "industry", "risk", "valuation"]

        self.selected_agents = selected_agents
        logger.info(f"[ChiefAgent] 开始机构级综合投研分析: {stock_code} | 选中Agent: {selected_agents}")

        try:
            connector = self.data_connector or DataConnector(stock_code)
        except Exception as e:
            error_msg = f"数据连接器初始化失败：{str(e)}"
            logger.error(f"[ChiefAgent] {error_msg}")
            er = error_report("system", error_msg).to_dict()
            return {
                "final_report": f"# {stock_code} 深度投研策略报告\n\n⚠️ {error_msg}",
                "reports": {d: error_report(d, error_msg).to_dict() for d in selected_agents},
                "chart_data": {},
                "overall_grade": "数据不可用",
                "overall_score": 0,
                "context_text": "",
                "_error": True,
            }

        data_start = time.time()
        try:
            all_data = connector.fetch_all()
            tech_data = all_data.get("tech_data")
            fundamental_data = all_data.get("fundamental_data") or all_data.get("financial_data")
            capital_data = all_data.get("capital_data")
            valuation_data = all_data.get("valuation_data")
            financial_data = all_data.get("financial_data") or all_data.get("fundamental_data")
            logger.info(f"[ChiefAgent] 数据获取完成: {stock_code} | 耗时: {time.time() - data_start:.1f}s")
        except Exception as e:
            error_msg = f"数据获取失败：{str(e)}"
            logger.error(f"[ChiefAgent] {error_msg}")
            return {
                "final_report": f"# {stock_code} 深度投研策略报告\n\n⚠️ {error_msg}",
                "reports": {d: error_report(d, error_msg).to_dict() for d in selected_agents},
                "chart_data": {},
                "overall_grade": "数据不可用",
                "overall_score": 0,
                "context_text": "",
                "_error": True,
            }

        validate_start = time.time()
        quality_report = data_validator.validate_all(all_data)
        quality_context = quality_report.to_context_string()
        logger.info(
            f"[ChiefAgent] 数据质量校验完成: {stock_code} | 评分: {quality_report.overall_score}/100 ({quality_report.overall_grade}) | 耗时: {time.time() - validate_start:.1f}s"
        )

        # === 知识库：注入历史跟踪上下文 + 研报共识 ===
        mem_start = time.time()
        try:
            tracking_context = build_tracking_context(stock_code)
            report_consensus = get_report_consensus(stock_code)
        except Exception as e:
            logger.warning(f"[ChiefAgent] 知识库查询失败: {e}")
            tracking_context = ""
            report_consensus = ""

        full_context_parts = [quality_context]
        if tracking_context and "首次分析" not in tracking_context:
            full_context_parts.append(tracking_context)
        if report_consensus and "无存量研报" not in report_consensus:
            full_context_parts.append(report_consensus)
        full_context = "\n\n".join(full_context_parts)
        logger.info(f"[ChiefAgent] 知识库上下文注入完成 | 耗时: {time.time() - mem_start:.1f}s")

        agent_start = time.time()
        logger.info(f"[ChiefAgent] 各维度Agent并行分析开始 | 选中: {selected_agents}")

        agents_map = {
            "tech": (TechAgent, {"tech_data": tech_data, "quality_context": full_context}),
            "fund": (FundAgent, {"fundamental_data": fundamental_data, "quality_context": full_context}),
            "capital": (CapitalAgent, {"capital_data": capital_data, "quality_context": full_context}),
            "industry": (IndustryAgent, {"fundamental_data": fundamental_data, "quality_context": full_context}),
            "risk": (RiskAgent, {"financial_data": financial_data, "tech_data": tech_data, "quality_context": full_context}),
            "valuation": (ValuationAgent, {"valuation_data": valuation_data, "fundamental_data": fundamental_data, "quality_context": full_context}),
        }

        active_agents = {k: v for k, v in agents_map.items() if k in selected_agents}
        raw_reports = {}

        if self.use_scheduler and self.scheduler is not None:
            # ── 新路径：HarnessScheduler 编排（带状态管理/重试/检查点） ──
            logger.info("[ChiefAgent] 使用 HarnessScheduler 编排分析阶段")
            try:
                raw_reports = self.scheduler.run_analysis_agents(
                    stock_code=stock_code,
                    all_data=all_data,
                    selected_agents=selected_agents,
                    model_name=self.model_name,
                    quality_context=full_context,
                )
            except Exception as e:
                logger.error(
                    f"[ChiefAgent] Scheduler 执行异常，回退到 legacy 模式: {e}"
                )
                raw_reports = self._run_agents_legacy(
                    active_agents, stock_code
                )

            # 质量门禁：低分报告触发重试
            try:
                from harness.quality_gate import QualityGate
                gate = QualityGate()
                raw_reports = gate.evaluate_and_retry(
                    reports=raw_reports,
                    stock_code=stock_code,
                    all_data=all_data,
                    scheduler=self.scheduler,
                    model_name=self.model_name,
                )
            except Exception as e:
                logger.warning(f"[ChiefAgent] 质量门禁异常，跳过: {e}")
        else:
            # ── 兼容路径：原 ThreadPoolExecutor 并行块 ──
            raw_reports = self._run_agents_legacy(active_agents, stock_code)

        for name in selected_agents:
            if name not in raw_reports:
                raw_reports[name] = error_report(name, "分析未执行").to_dict()

        reports = {}
        for name, raw in raw_reports.items():
            if isinstance(raw, dict):
                reports[name] = AgentReport.from_dict(raw, name)
            else:
                reports[name] = AgentReport(
                    dimension=name, overall_score=50, grade="中性",
                    confidence=0, thesis="未知格式返回",
                    key_signals=[], risk_factors=[], recommendation="",
                    raw_text=str(raw)[:500], parse_error=True
                )

        logger.info(f"[ChiefAgent] 并行分析完成 | 耗时: {time.time() - agent_start:.1f}s")

        # ====== Phase 2+3: 多智能体辩论（交叉审阅 + 修订轮） ======
        debate_start = time.time()
        try:
            debate_reports, debate_results = run_debate_rounds(reports, self.model_name)

            debate_summary = build_debate_summary(reports, debate_results)

            reports = debate_reports

            changed_count = sum(1 for r in debate_results.values() if r.changed)
            logger.info(
                f"[ChiefAgent] 辩论完成 | {changed_count} 个Agent修订评分 | 耗时: {time.time() - debate_start:.1f}s"
            )
        except Exception as e:
            logger.warning(f"[ChiefAgent] 辩论流程异常，使用原始报告: {e}")
            debate_summary = ""

        final_report = self.synthesize_reports(stock_code, reports)

        if debate_summary:
            final_report += "\n\n---\n\n" + debate_summary

        # 构建图表数据
        from chart_builder import chart_builder
        reports_dict = {name: r.to_dict() for name, r in reports.items()}
        chart_data = chart_builder.build(stock_code, all_data, reports_dict)

        # 构建对话上下文（精简版分析摘要）
        aggregation = aggregate_reports(reports)
        context_text = self._build_context_text(stock_code, aggregation, reports_dict)

        total_time = time.time() - start_time
        logger.info(f"[ChiefAgent] 机构级综合投研分析完成: {stock_code} | 总耗时: {total_time:.1f}s | 图表类型: {len(chart_data)}")

        # === 知识库持久化：保存分析快照 ===
        try:
            dim_scores = {}
            for dim_name, info in aggregation.get("dimensions", {}).items():
                dim_scores[dim_name] = info.get("score", 50)

            price = None
            basic_info = all_data.get("basic_info", {}) or {}
            if basic_info:
                price = basic_info.get("最新价") or basic_info.get("price")

            key_conclusion = ""
            aggregation_subset = aggregation or {}
            all_signals = aggregation_subset.get("all_signals", []) or []

            if final_report:
                for line in final_report.split("\n"):
                    stripped = line.strip()
                    if stripped.startswith("**核心结论**") or stripped.startswith("核心结论"):
                        key_conclusion = stripped.lstrip("*").strip()
                        break
                if not key_conclusion and all_signals:
                    key_conclusion = all_signals[0]

            save_snapshot(
                stock_code=stock_code,
                overall_score=aggregation.get("overall_score", 50),
                overall_grade=aggregation.get("overall_grade", "中性"),
                dimension_scores=dim_scores,
                key_conclusion=key_conclusion,
                top_signals=all_signals[:5],
                top_risks=(aggregation.get("all_risks", []) or [])[:5],
                price_at_analysis=price,
                full_report_md=final_report,
            )
            logger.info(f"[ChiefAgent] 分析快照已存储: {stock_code}")
        except Exception as e:
            logger.warning(f"[ChiefAgent] 知识库存储失败: {e}")

        try:
            from layers.memory.knowledge_base import save_analysis_as_events
            event_count = save_analysis_as_events(stock_code, aggregation, final_report)
            if event_count:
                logger.info(f"[ChiefAgent] 事件时间线已存储: {stock_code} ({event_count}条)")
        except Exception as e:
            logger.warning(f"[ChiefAgent] 事件时间线存储失败: {e}")

        return {
            "final_report": final_report,
            "reports": reports_dict,
            "chart_data": chart_data,
            "overall_grade": aggregation.get("overall_grade", "中性"),
            "overall_score": aggregation.get("overall_score", 50),
            "context_text": context_text,
        }

    def _run_agents_legacy(
        self,
        active_agents: Dict[str, tuple],
        stock_code: str,
    ) -> Dict[str, Dict]:
        """兼容路径：原 ThreadPoolExecutor 并行执行块

        当 use_scheduler=False 或 scheduler 执行异常时使用。
        """
        raw_reports = {}
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as executor:
            future_to_agent = {}
            for name, (AgentClass, sub_state) in active_agents.items():
                agent = AgentClass(model_name=self.model_name)
                future = executor.submit(agent.analyze, stock_code, sub_state)
                future_to_agent[future] = name

            for future in as_completed(future_to_agent):
                agent_name = future_to_agent[future]
                try:
                    result = future.result()
                    # 统一转为 dict
                    if isinstance(result, dict):
                        raw_reports[agent_name] = result
                    elif hasattr(result, "to_dict"):
                        raw_reports[agent_name] = result.to_dict()
                    else:
                        raw_reports[agent_name] = error_report(
                            agent_name, "未知格式返回"
                        ).to_dict()
                except Exception as e:
                    error_msg = f"{agent_name}分析异常：{str(e)}"
                    logger.error(f"[ChiefAgent] {error_msg}")
                    raw_reports[agent_name] = error_report(agent_name, error_msg).to_dict()
        return raw_reports

    def synthesize_reports(self, stock_code: str, reports: Dict[str, AgentReport]) -> str:
        logger.info(f"[ChiefAgent] 开始综合研报汇总: {stock_code}")

        aggregation = aggregate_reports(reports)

        dimension_md = reports_to_markdown(reports)

        dim_names = {"tech": "技术面", "fund": "基本面", "capital": "资金面",
                     "industry": "行业面", "risk": "风险面", "valuation": "估值面"}

        dim_scores_text = []
        for dim_name, info in aggregation.get("dimensions", {}).items():
            label = dim_names.get(dim_name, dim_name)
            dim_scores_text.append(
                f"- {label}: 评分 {info['score']}/100 | {info['grade']} | 置信度 {info['confidence']}% | {info['thesis']}"
            )

        invalid_text = ""
        if aggregation.get("invalid_dimensions"):
            invalid_text = "\n【数据不可用维度】\n"
            for dim, reason in aggregation["invalid_dimensions"].items():
                label = dim_names.get(dim, dim)
                invalid_text += f"- {label}: {reason}\n"

        prompt = f"""你是一位资深首席策略分析师，请基于以下各维度的结构化分析数据，撰写一份专业的综合投研策略报告。

股票代码：{stock_code}

【综合评分概览（规则引擎聚合）】
综合评分：{aggregation['overall_score']}/100
综合评级：{aggregation['overall_grade']}
综合置信度：{aggregation['overall_confidence']}%
有效维度数：{aggregation['valid_count']}/{aggregation['dimension_count']}
多空共识：{aggregation['consensus']}

【各维度评分明细】
{chr(10).join(dim_scores_text) if dim_scores_text else '无有效维度数据'}

【跨维度关键信号汇总】
{chr(10).join(f'- {s}' for s in aggregation.get('all_signals', [])) if aggregation.get('all_signals') else '无'}

【跨维度风险汇总】
{chr(10).join(f'- {r}' for r in aggregation.get('all_risks', [])) if aggregation.get('all_risks') else '无'}

【多空分歧】
{chr(10).join(f'- {c}' for c in aggregation.get('conflicts', [])) if aggregation.get('conflicts') else '各维度信号方向基本一致'}
{invalid_text}

【各维度详细分析】
{dimension_md}

请按以下步骤逐步推理，再进行报告撰写：

分析推理步骤（必须按此顺序分步思考）：
Step 1: 通览各维度评分，计算各维度信号的一致性/分歧度，标注矛盾维度
Step 2: 判断多空力量对比，基于各维度共识和分歧，计算综合多空倾向
Step 3: 筛选最关键的2-3个风险点（从all_risks中优先选得分最低维度的风险）
Step 4: 推演乐观/中性/悲观三种情景下的投资路径
Step 5: 基于以上四步，制定具体的操作策略建议

请按以下结构输出综合策略报告（1200-1500字）：
1. **核心结论** - 基于综合评分 {aggregation['overall_score']}/100 和 综合评级 {aggregation['overall_grade']}，给出明确的综合研判
2. **多维度交叉验证** - 逐一分析各维度的信号方向是否一致，重点标注信号冲突的维度
3. **关键矛盾与风险** - 汇总跨维度风险，分析最可能影响投资决策的2-3个关键矛盾点
4. **情景分析** - 乐观/中性/悲观三种情景下的投资逻辑推演
5. **操作策略建议** - 具体的仓位、止损、目标价建议

要求：专业、客观、严格基于提供的数据，使用机构级术语。综合评分和评级来自规则引擎的量化聚合，请在报告中体现这一量化维度。"""

        try:
            completion = self.client.chat.completions.create(
                model=get_model_id(self.model_name),
                messages=[
                    {"role": "system", "content": "你是一位资深首席策略分析师，擅长多维度交叉验证和综合研判。请严格基于提供的数据进行分析，不要编造任何未提供的数据。如果数据不足，请明确指出并降低结论确定性。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=LLM_TEMPERATURE,
                max_tokens=REPORT_MAX_TOKENS
            )
            final_narrative = completion.choices[0].message.content.strip()

            header = f"""# {stock_code} 深度投研策略报告

## 综合评分概览

| 维度 | 评分 | 评级 | 置信度 |
|------|------|------|--------|
"""
            for dim_name, info in aggregation.get("dimensions", {}).items():
                label = dim_names.get(dim_name, dim_name)
                header += f"| {label} | {info['score']}/100 | {info['grade']} | {info['confidence']}% |\n"

            header += f"""
| **综合** | **{aggregation['overall_score']}/100** | **{aggregation['overall_grade']}** | **{aggregation['overall_confidence']}%** |

> 多空共识：{aggregation['consensus']} | 有效维度：{aggregation['valid_count']}/{aggregation['dimension_count']}

---

## 首席策略研判

"""

            if aggregation.get("conflicts"):
                header += "### ⚠️ 多空分歧提示\n\n"
                for c in aggregation["conflicts"]:
                    header += f"> {c}\n"
                header += "\n"

            final_report = header + final_narrative

            logger.info(f"[ChiefAgent] 综合研报汇总完成: {stock_code}")
            return final_report
        except Exception as e:
            error_msg = f"LLM综合汇总生成失败：{str(e)}"
            logger.error(f"[ChiefAgent] {error_msg}")

            header = f"""# {stock_code} 深度投研策略报告

## 综合评分概览

| 维度 | 评分 | 评级 | 置信度 |
|------|------|------|--------|
"""
            for dim_name, info in aggregation.get("dimensions", {}).items():
                label = dim_names.get(dim_name, dim_name)
                header += f"| {label} | {info['score']}/100 | {info['grade']} | {info['confidence']}% |\n"

            header += f"""
| **综合** | **{aggregation['overall_score']}/100** | **{aggregation['overall_grade']}** | **{aggregation['overall_confidence']}%** |

> ⚠️ LLM综合汇总生成失败：{error_msg}

---

## 各维度结构化分析

{dimension_md}
"""
            return header

    def _build_context_text(self, stock_code: str, aggregation: Dict,
                            reports_dict: Dict) -> str:
        """构建对话上下文精简摘要，供ChatEngine使用"""
        dim_names = {"tech": "技术面", "fund": "基本面", "capital": "资金面",
                     "industry": "行业面", "risk": "风险面", "valuation": "估值面"}

        lines = [
            f"股票代码：{stock_code}",
            f"综合评分：{aggregation.get('overall_score', 50)}/100",
            f"综合评级：{aggregation.get('overall_grade', '中性')}",
            f"多空共识：{aggregation.get('consensus', '无数据')}",
            f"有效维度：{aggregation.get('valid_count', 0)}/{aggregation.get('dimension_count', 0)}",
            "",
            "各维度摘要：",
        ]

        for dim_name, info in aggregation.get("dimensions", {}).items():
            label = dim_names.get(dim_name, dim_name)
            lines.append(
                f"- {label}：{info['score']}分 {info['grade']} | {info['thesis']}"
            )
            for sig in info.get("signals", [])[:2]:
                lines.append(f"  · {sig}")
            for risk in info.get("risks", [])[:2]:
                lines.append(f"  · 风险：{risk}")

        if aggregation.get("conflicts"):
            lines.append("")
            lines.append("信号分歧：")
            for c in aggregation["conflicts"]:
                lines.append(f"  · {c}")

        return "\n".join(lines)


def chief_agent_node(state: dict) -> dict:
    stock_code = state["stock_code"]
    agent = ChiefAgent(model_name=DEFAULT_MODEL)
    result = agent.analyze(stock_code, state)
    if isinstance(result, dict):
        state["final_report"] = result.get("final_report", "")
    else:
        state["final_report"] = result
    return state


chief_agent = ChiefAgent