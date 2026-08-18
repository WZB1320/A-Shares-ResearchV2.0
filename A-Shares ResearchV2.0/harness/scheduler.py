import sys
import logging
import time
import uuid
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from harness.state import HarnessStateManager, TaskStatus

logger = logging.getLogger("Harness-Scheduler")


@dataclass
class AgentSpec:
    """Agent 注册规格：封装 Agent 类 + 数据负载构建器 + 依赖"""
    agent_cls: type
    data_payload_builder: Callable[[Dict], Dict]
    dependencies: List[str] = field(default_factory=list)


class SchedulerConfig:
    def __init__(
        self,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        parallel_execution: bool = True,
        max_workers: int = 4,
        validation_enabled: bool = True,
    ):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.parallel_execution = parallel_execution
        self.max_workers = max_workers
        # 保留字段以兼容构造参数；数据质量校验已统一迁移到 layers.validators，
        # 由 ChiefAgent 在调度前调用，本调度器不再内置校验环节。
        self.validation_enabled = validation_enabled


class HarnessScheduler:
    def __init__(self, config: Optional[SchedulerConfig] = None):
        self.config = config or SchedulerConfig()
        # 仅保留引用便于调试/兜底，正确性不再依赖此属性（状态通过参数显式传递）。
        self.state_manager: Optional[HarnessStateManager] = None
        # 新式注册：name -> AgentSpec（供 run_analysis_agents 使用）
        self.agent_specs: Dict[str, AgentSpec] = {}
        self.agent_dependencies: Dict[str, List[str]] = {}

    def register_agent(
        self,
        name: str,
        agent: type,
        data_payload_builder: Optional[Callable[[Dict], Dict]] = None,
        dependencies: Optional[List[str]] = None,
    ) -> None:
        """注册 Agent（仅支持 Agent 类 + data_payload_builder）。

        旧式 Callable 注册与 run()/run_workflow() 接口已在清理中移除：
        并发安全的新式编排入口为 run_analysis_agents()，通过请求级 session_id
        隔离状态，不再复用常驻的 self.state_manager。
        """
        if not isinstance(agent, type):
            raise TypeError(
                f"register_agent('{name}') 仅接受 Agent 类，旧式 Callable 注册已废弃"
            )
        if data_payload_builder is None:
            raise ValueError(
                f"注册 Agent 类 {name} 时必须提供 data_payload_builder"
            )
        deps = dependencies or []
        self.agent_specs[name] = AgentSpec(
            agent_cls=agent,
            data_payload_builder=data_payload_builder,
            dependencies=deps,
        )
        self.agent_dependencies[name] = deps
        logger.info(f"[Scheduler] 注册Agent: {name} | 依赖: {deps} | 模式: spec")

    def unregister_agent(self, name: str) -> None:
        self.agent_specs.pop(name, None)
        self.agent_dependencies.pop(name, None)
        logger.info(f"[Scheduler] 注销Agent: {name}")

    def _new_session_id(self, stock_code: str) -> str:
        """生成请求级唯一会话ID（并发隔离关键）。"""
        suffix = uuid.uuid4().hex[:8]
        return f"{stock_code}_{int(time.time() * 1000)}_{suffix}"

    def run_analysis_agents(
        self,
        stock_code: str,
        all_data: Dict,
        selected_agents: List[str],
        model_name: str,
        quality_context: str = "",
        session_id: Optional[str] = None,
    ) -> Dict[str, Dict]:
        """执行分析 Agents，返回 {agent_name: report_dict}

        与 ChiefAgent 原有的 ThreadPoolExecutor 并行块功能等价，但增加了：
          - 状态管理（每个 Agent 的 start/complete/fail/retry 状态持久化）
          - 失败重试（按 config.max_retries）
          - 请求级状态隔离（独立 session_id，不复用常驻 self.state_manager）

        Args:
            stock_code: 股票代码
            all_data: DataConnector.fetch_all() 的完整数据
            selected_agents: 选中的 Agent 名称列表（如 ["tech", "fund", ...]）
            model_name: LLM 模型名
            quality_context: 数据质量上下文（注入到每个 Agent 的 state）
            session_id: 请求级会话ID（必填以隔离状态；缺省则自动生成）。
        Returns:
            {agent_name: report_dict} 字典，report_dict 为 AgentReport.to_dict()
        """
        logger.info(
            f"[Scheduler] ========== 分析调度 | stock={stock_code} | "
            f"agents={selected_agents} =========="
        )

        # 并发隔离修复：每个分析请求使用独立的状态管理器，
        # 不再复用 self.state_manager（旧逻辑下它被常驻单例持有，
        # 并发请求会互相覆盖 tasks/data，且 session_id 固定导致检查点互相覆盖）。
        session_id = session_id or self._new_session_id(stock_code)
        state_manager = HarnessStateManager(stock_code, session_id=session_id)
        # 仅保留引用便于调试/兜底，正确性不再依赖此属性（状态通过参数显式传递）。
        self.state_manager = state_manager

        state_manager.update_phase("ANALYSIS")
        # 缓存 all_data，供 data_payload_builder 使用
        state_manager.store_data("all_data", all_data)

        # 构建待执行任务列表
        tasks: List[tuple] = []
        for name in selected_agents:
            spec = self.agent_specs.get(name)
            if spec is None:
                logger.warning(f"[Scheduler] Agent {name} 未注册，跳过")
                continue
            data_payload = spec.data_payload_builder(all_data)
            if quality_context:
                data_payload["quality_context"] = quality_context
            tasks.append((name, spec, data_payload))

        # 并行执行（带重试 + 状态管理）
        results: Dict[str, Dict] = {}
        if self.config.parallel_execution and len(tasks) > 1:
            results = self._execute_specs_parallel(
                tasks, stock_code, model_name, state_manager
            )
        else:
            for name, spec, data_payload in tasks:
                results[name] = self._execute_spec_with_retry(
                    name, spec, stock_code, data_payload, model_name, state_manager
                )

        logger.info(
            f"[Scheduler] ========== 分析调度完成 | session={session_id} | "
            f"成功: {sum(1 for r in results.values() if r and not r.get('parse_error'))}/"
            f"{len(selected_agents)} =========="
        )
        return results

    def _execute_specs_parallel(
        self,
        tasks: List[tuple],
        stock_code: str,
        model_name: str,
        state_manager: HarnessStateManager,
    ) -> Dict[str, Dict]:
        """并行执行多个 AgentSpec 任务（状态管理器由调用方显式传入，保证请求隔离）"""
        results: Dict[str, Dict] = {}
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            future_to_name = {}
            for name, spec, data_payload in tasks:
                future = executor.submit(
                    self._execute_spec_with_retry,
                    name, spec, stock_code, data_payload, model_name, state_manager
                )
                future_to_name[future] = name

            for future in as_completed(future_to_name):
                name = future_to_name[future]
                try:
                    results[name] = future.result()
                except Exception as e:
                    logger.error(f"[Scheduler] {name} 执行失败: {e}")
                    try:
                        from layers.agents.report_schema import error_report
                        results[name] = error_report(name, str(e)).to_dict()
                    except ImportError:
                        results[name] = {
                            "dimension": name,
                            "overall_score": 0,
                            "grade": "数据不可用",
                            "thesis": f"执行异常: {e}",
                            "parse_error": True,
                        }
        return results

    def _execute_spec_with_retry(
        self,
        name: str,
        spec: AgentSpec,
        stock_code: str,
        data_payload: Dict,
        model_name: str,
        state_manager: HarnessStateManager,
    ) -> Dict:
        """执行单个 AgentSpec，带重试和状态管理（状态管理器由调用方显式传入）"""
        state_manager.start_task(name)

        last_error: Optional[Exception] = None
        for attempt in range(self.config.max_retries):
            try:
                agent = spec.agent_cls(model_name=model_name)
                report = agent.analyze(stock_code, data_payload)

                # 统一转为 dict 格式
                if isinstance(report, dict):
                    result = report
                elif hasattr(report, "to_dict"):
                    result = report.to_dict()
                else:
                    result = {
                        "dimension": name,
                        "overall_score": 50,
                        "grade": "中性",
                        "thesis": "未知格式返回",
                        "raw_text": str(report)[:500],
                        "parse_error": True,
                    }

                state_manager.complete_task(name, result)
                return result

            except Exception as e:
                last_error = e
                if attempt < self.config.max_retries - 1:
                    logger.warning(
                        f"[Scheduler] {name} 第 {attempt + 1} 次执行失败: {e}，重试中..."
                    )
                    state_manager.retry_task(name)
                    time.sleep(self.config.retry_delay * (attempt + 1))
                else:
                    logger.error(
                        f"[Scheduler] {name} 重试 {self.config.max_retries} 次后仍失败: {e}"
                    )

        # 所有重试失败
        error_msg = f"{type(last_error).__name__}: {str(last_error)}" if last_error else "未知错误"
        state_manager.fail_task(name, error_msg)

        try:
            from layers.agents.report_schema import error_report
            return error_report(name, error_msg).to_dict()
        except ImportError:
            return {
                "dimension": name,
                "overall_score": 0,
                "grade": "数据不可用",
                "thesis": error_msg,
                "parse_error": True,
            }

    def get_execution_plan(self) -> List[Dict[str, Any]]:
        plan = []
        for agent_name in self.agent_specs.keys():
            plan.append({
                "agent": agent_name,
                "dependencies": self.agent_dependencies.get(agent_name, []),
                "can_parallel": all(
                    self.state_manager.is_task_completed(dep)
                    for dep in self.agent_dependencies.get(agent_name, [])
                ) if self.state_manager else False,
            })
        return plan


harness_scheduler = HarnessScheduler()
