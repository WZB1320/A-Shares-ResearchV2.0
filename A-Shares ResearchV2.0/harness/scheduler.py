import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent))

from harness.state import HarnessStateManager, TaskStatus
from harness.validator import harness_validator, ValidationResult

logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] %(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("Harness-Scheduler")


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
        self.agents: Dict[str, Callable] = {}
        self.agent_dependencies: Dict[str, List[str]] = {}

    def register_agent(self, name: str, agent_fn: Callable, dependencies: Optional[List[str]] = None) -> None:
        self.agents[name] = agent_fn
        self.agent_dependencies[name] = dependencies or []
        logger.info(f"[Scheduler] 注册Agent: {name} | 依赖: {dependencies}")

    def unregister_agent(self, name: str) -> None:
        if name in self.agents:
            del self.agents[name]
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


harness_scheduler = HarnessScheduler()
