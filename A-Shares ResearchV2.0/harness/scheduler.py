import sys
import logging
import time
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List, Callable, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from harness.state import HarnessStateManager, TaskStatus
from harness.validator import harness_validator, ValidationResult

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
        checkpoint_enabled: bool = True,
        validation_enabled: bool = True
    ):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.parallel_execution = parallel_execution
        self.max_workers = max_workers
        self.checkpoint_enabled = checkpoint_enabled
        self.validation_enabled = validation_enabled


class AgentExecutor:
    def __init__(self, agent_fn: Callable, agent_name: str, state_manager: HarnessStateManager):
        self.agent_fn = agent_fn
        self.agent_name = agent_name
        self.state_manager = state_manager

    def execute(self, stock_code: str, **kwargs) -> Any:
        self.state_manager.start_task(self.agent_name)

        try:
            result = self.agent_fn(stock_code=stock_code, **kwargs)
            self.state_manager.complete_task(self.agent_name, result)
            return result
        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            self.state_manager.fail_task(self.agent_name, error_msg)
            raise


class HarnessScheduler:
    def __init__(self, config: Optional[SchedulerConfig] = None):
        self.config = config or SchedulerConfig()
        self.state_manager: Optional[HarnessStateManager] = None
        # 旧式注册：name -> Callable
        self.agents: Dict[str, Callable] = {}
        self.agent_dependencies: Dict[str, List[str]] = {}
        # 新式注册：name -> AgentSpec（用于 run_analysis_agents）
        self.agent_specs: Dict[str, AgentSpec] = {}

    def register_agent(
        self,
        name: str,
        agent: Union[Callable, type],
        data_payload_builder: Optional[Callable[[Dict], Dict]] = None,
        dependencies: Optional[List[str]] = None,
    ) -> None:
        """注册 Agent

        兼容两种模式：
          1. 旧式：agent 为 Callable，用于 run()/run_workflow()
          2. 新式：agent 为 Agent 类，配合 data_payload_builder 用于 run_analysis_agents()
        """
        deps = dependencies or []
        if data_payload_builder is not None or isinstance(agent, type):
            # 新式注册
            if data_payload_builder is None:
                raise ValueError(
                    f"注册 Agent 类 {name} 时必须提供 data_payload_builder"
                )
            self.agent_specs[name] = AgentSpec(
                agent_cls=agent,
                data_payload_builder=data_payload_builder,
                dependencies=deps,
            )
            # 同时注册一个包装 Callable，保持旧式 run() 可用
            self.agents[name] = self._wrap_spec_as_callable(name)
        else:
            # 旧式注册
            self.agents[name] = agent
        self.agent_dependencies[name] = deps
        logger.info(f"[Scheduler] 注册Agent: {name} | 依赖: {deps} | 模式: "
                    f"{'spec' if name in self.agent_specs else 'callable'}")

    def _wrap_spec_as_callable(self, name: str) -> Callable:
        """将 AgentSpec 包装为旧式 Callable，供 run() 使用"""
        spec = self.agent_specs[name]

        def wrapped(stock_code: str, **kwargs):
            # 优先使用 kwargs 中传入的数据，否则从 state_manager 取
            all_data = kwargs.pop("all_data", None)
            if all_data is None and self.state_manager:
                all_data = self.state_manager.get_data("all_data") or {}
            data_payload = spec.data_payload_builder(all_data or {})
            data_payload.update(kwargs)
            agent = spec.agent_cls()
            return agent.analyze(stock_code, data_payload)

        return wrapped

    def unregister_agent(self, name: str) -> None:
        if name in self.agents:
            del self.agents[name]
        if name in self.agent_specs:
            del self.agent_specs[name]
        if name in self.agent_dependencies:
            del self.agent_dependencies[name]
        logger.info(f"[Scheduler] 注销Agent: {name}")

    def _check_dependencies_met(self, agent_name: str) -> bool:
        dependencies = self.agent_dependencies.get(agent_name, [])
        for dep in dependencies:
            if not self.state_manager.is_task_completed(dep):
                logger.warning(f"[Scheduler] Agent {agent_name} 依赖未满足: {dep}")
                return False
        return True

    def _execute_single_agent(self, agent_name: str, stock_code: str, **kwargs) -> Any:
        if not self._check_dependencies_met(agent_name):
            self.state_manager.skip_task(agent_name, "依赖未满足")
            return None

        executor = AgentExecutor(self.agents[agent_name], agent_name, self.state_manager)

        for attempt in range(self.config.max_retries):
            try:
                result = executor.execute(stock_code, **kwargs)
                return result
            except Exception as e:
                if attempt < self.config.max_retries - 1:
                    logger.warning(f"[Scheduler] Agent {agent_name} 执行失败，第 {attempt + 1} 次重试")
                    self.state_manager.retry_task(agent_name)
                    import time
                    time.sleep(self.config.retry_delay * (attempt + 1))
                else:
                    logger.error(f"[Scheduler] Agent {agent_name} 重试次数用尽")
                    raise

        return None

    def _execute_parallel_agents(self, agent_names: List[str], stock_code: str, **kwargs) -> Dict[str, Any]:
        results = {}

        if not self.config.parallel_execution:
            for agent_name in agent_names:
                results[agent_name] = self._execute_single_agent(agent_name, stock_code, **kwargs)
            return results

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            future_to_agent = {
                executor.submit(self._execute_single_agent, name, stock_code, **kwargs): name
                for name in agent_names
            }

            for future in as_completed(future_to_agent):
                agent_name = future_to_agent[future]
                try:
                    results[agent_name] = future.result()
                except Exception as e:
                    logger.error(f"[Scheduler] Agent {agent_name} 并行执行异常: {e}")
                    results[agent_name] = None

        return results

    def _validate_task_results(self, results: Dict[str, Any]) -> Dict[str, ValidationResult]:
        validation_results = {}

        for agent_name, result in results.items():
            if result is None:
                continue

            if isinstance(result, dict):
                if "report" in result:
                    validation_results[agent_name] = harness_validator.validate_report_content(result["report"])
                elif "valuation" in result:
                    validation_results[agent_name] = harness_validator.validate_valuation(result["valuation"])

        return validation_results

    def run(self, stock_code: str, **kwargs) -> HarnessStateManager:
        logger.info(f"[Scheduler] ========== 开始调度 | stock_code={stock_code} ==========")

        self.state_manager = HarnessStateManager(stock_code)
        self.state_manager.update_phase("DATA_FETCH")

        data_agent_name = "data_fetcher"
        if data_agent_name in self.agents:
            self._execute_single_agent(data_agent_name, stock_code, **kwargs)

        if self.config.checkpoint_enabled:
            self.state_manager.save_checkpoint()

        analysis_agents = [name for name in self.agents.keys() if name != data_agent_name]

        if not analysis_agents:
            logger.warning("[Scheduler] 没有注册的分析师Agent")
            return self.state_manager

        self.state_manager.update_phase("ANALYSIS")

        logger.info(f"[Scheduler] 开始并行执行 {len(analysis_agents)} 个分析师Agent")
        analysis_results = self._execute_parallel_agents(analysis_agents, stock_code, **kwargs)

        if self.config.validation_enabled:
            self.state_manager.update_phase("VALIDATION")
            validation_results = self._validate_task_results(analysis_results)
            logger.info(f"[Scheduler] 验证完成 | 结果数: {len(validation_results)}")

        if self.config.checkpoint_enabled:
            self.state_manager.save_checkpoint()

        self.state_manager.update_phase("COMPLETED")
        logger.info(f"[Scheduler] ========== 调度完成 ==========")
        logger.info(f"[Scheduler] 执行摘要: {self.state_manager.get_summary()}")

        return self.state_manager

    def run_workflow(self, stock_code: str, workflow: List[str], **kwargs) -> HarnessStateManager:
        logger.info(f"[Scheduler] ========== 开始工作流调度 | stock_code={stock_code} ==========")

        self.state_manager = HarnessStateManager(stock_code)
        self.state_manager.update_phase("WORKFLOW")

        for phase_name, agent_names in workflow:
            self.state_manager.update_phase(phase_name)
            logger.info(f"[Scheduler] 执行阶段: {phase_name} | Agents: {agent_names}")

            if isinstance(agent_names, list):
                self._execute_parallel_agents(agent_names, stock_code, **kwargs)
            else:
                self._execute_single_agent(agent_names, stock_code, **kwargs)

            if self.config.checkpoint_enabled:
                self.state_manager.save_checkpoint()

        self.state_manager.update_phase("COMPLETED")
        logger.info(f"[Scheduler] ========== 工作流完成 ==========")

        return self.state_manager

    def resume_from_checkpoint(self, session_id: str) -> bool:
        if not self.state_manager:
            self.state_manager = HarnessStateManager("")

        if self.state_manager.load_checkpoint(session_id):
            logger.info(f"[Scheduler] 从检查点恢复会话: {session_id}")
            return True
        return False

    def get_execution_plan(self) -> List[Dict[str, Any]]:
        plan = []
        for agent_name in self.agents.keys():
            plan.append({
                "agent": agent_name,
                "dependencies": self.agent_dependencies.get(agent_name, []),
                "can_parallel": all(
                    self.state_manager.is_task_completed(dep)
                    for dep in self.agent_dependencies.get(agent_name, [])
                ) if self.state_manager else False
            })
        return plan

    # ── 新式编排接口：供 ChiefAgent 使用 ──────────────────────

    def run_analysis_agents(
        self,
        stock_code: str,
        all_data: Dict,
        selected_agents: List[str],
        model_name: str,
        quality_context: str = "",
    ) -> Dict[str, Dict]:
        """执行分析 Agents，返回 {agent_name: report_dict}

        与 ChiefAgent 原有的 ThreadPoolExecutor 并行块功能等价，但增加了：
          - 状态管理（每个 Agent 的 start/complete/fail/retry 状态持久化）
          - 失败重试（按 config.max_retries）
          - 检查点保存

        Args:
            stock_code: 股票代码
            all_data: DataConnector.fetch_all() 的完整数据
            selected_agents: 选中的 Agent 名称列表（如 ["tech", "fund", ...]）
            model_name: LLM 模型名
            quality_context: 数据质量上下文（注入到每个 Agent 的 state）

        Returns:
            {agent_name: report_dict} 字典，report_dict 为 AgentReport.to_dict()
        """
        logger.info(
            f"[Scheduler] ========== 分析调度 | stock={stock_code} | "
            f"agents={selected_agents} =========="
        )

        # 初始化状态管理器（不覆盖 ChiefAgent 已有的状态管理）
        if self.state_manager is None:
            self.state_manager = HarnessStateManager(stock_code)
        self.state_manager.update_phase("ANALYSIS")
        # 缓存 all_data，供 _wrap_spec_as_callable 使用
        self.state_manager.store_data("all_data", all_data)

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
                tasks, stock_code, model_name
            )
        else:
            for name, spec, data_payload in tasks:
                results[name] = self._execute_spec_with_retry(
                    name, spec, stock_code, data_payload, model_name
                )

        if self.config.checkpoint_enabled:
            self.state_manager.save_checkpoint()

        logger.info(
            f"[Scheduler] ========== 分析调度完成 | "
            f"成功: {sum(1 for r in results.values() if r and not r.get('parse_error'))}/"
            f"{len(selected_agents)} =========="
        )
        return results

    def _execute_specs_parallel(
        self,
        tasks: List[tuple],
        stock_code: str,
        model_name: str,
    ) -> Dict[str, Dict]:
        """并行执行多个 AgentSpec 任务"""
        results: Dict[str, Dict] = {}
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            future_to_name = {}
            for name, spec, data_payload in tasks:
                future = executor.submit(
                    self._execute_spec_with_retry,
                    name, spec, stock_code, data_payload, model_name
                )
                future_to_name[future] = name

            for future in as_completed(future_to_name):
                name = future_to_name[future]
                try:
                    results[name] = future.result()
                except Exception as e:
                    logger.error(f"[Scheduler] {name} 执行失败: {e}")
                    # 返回错误报告，保持与 ChiefAgent 原逻辑一致
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
    ) -> Dict:
        """执行单个 AgentSpec，带重试和状态管理"""
        self.state_manager.start_task(name)

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

                self.state_manager.complete_task(name, result)
                return result

            except Exception as e:
                last_error = e
                if attempt < self.config.max_retries - 1:
                    logger.warning(
                        f"[Scheduler] {name} 第 {attempt + 1} 次执行失败: {e}，重试中..."
                    )
                    self.state_manager.retry_task(name)
                    time.sleep(self.config.retry_delay * (attempt + 1))
                else:
                    logger.error(
                        f"[Scheduler] {name} 重试 {self.config.max_retries} 次后仍失败: {e}"
                    )

        # 所有重试失败
        error_msg = f"{type(last_error).__name__}: {str(last_error)}" if last_error else "未知错误"
        self.state_manager.fail_task(name, error_msg)

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


harness_scheduler = HarnessScheduler()
