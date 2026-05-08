import sys
import json
import logging
from pathlib import Path
from typing import Dict, TypedDict, Optional, List, Any
from datetime import datetime
from enum import Enum

logger = logging.getLogger("Harness-State")


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRY = "retry"
    SKIPPED = "skipped"


class AgentTaskRecord(TypedDict):
    agent_name: str
    status: str
    start_time: Optional[str]
    end_time: Optional[str]
    result: Optional[Any]
    error: Optional[str]
    retry_count: int
    execution_time: Optional[float]


class HarnessState(TypedDict):
    stock_code: str
    session_id: str
    created_at: str
    updated_at: str
    current_phase: str
    tasks: Dict[str, AgentTaskRecord]
    data: Dict[str, Any]
    reports: Dict[str, str]
    final_report: Optional[str]
    metadata: Dict[str, Any]


class HarnessStateManager:
    def __init__(self, stock_code: str, session_id: Optional[str] = None):
        self.stock_code = stock_code
        self.session_id = session_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.state: HarnessState = self._create_initial_state()
        self._checkpoint_dir = Path("checkpoints")
        self._checkpoint_dir.mkdir(exist_ok=True)
        self._log_dir = Path("logs")
        self._log_dir.mkdir(exist_ok=True)
        logger.info(f"[State] 初始化状态管理器 | stock_code={stock_code} | session_id={self.session_id}")

    def _create_initial_state(self) -> HarnessState:
        return HarnessState(
            stock_code=self.stock_code,
            session_id=self.session_id,
            created_at=datetime.now().isoformat(),
            updated_at=datetime.now().isoformat(),
            current_phase="INIT",
            tasks={},
            data={},
            reports={},
            final_report=None,
            metadata={}
        )

    def update_phase(self, phase: str) -> None:
        self.state["current_phase"] = phase
        self.state["updated_at"] = datetime.now().isoformat()
        logger.info(f"[State] 阶段更新: {phase}")

    def start_task(self, agent_name: str) -> None:
        self.state["tasks"][agent_name] = AgentTaskRecord(
            agent_name=agent_name,
            status=TaskStatus.RUNNING.value,
            start_time=datetime.now().isoformat(),
            end_time=None,
            result=None,
            error=None,
            retry_count=0,
            execution_time=None
        )
        self.state["updated_at"] = datetime.now().isoformat()
        logger.info(f"[State] 任务开始: {agent_name}")

    def complete_task(self, agent_name: str, result: Any) -> None:
        if agent_name not in self.state["tasks"]:
            self.start_task(agent_name)

        task = self.state["tasks"][agent_name]
        end_time = datetime.now()
        start_time = datetime.fromisoformat(task["start_time"]) if task["start_time"] else end_time

        task["status"] = TaskStatus.COMPLETED.value
        task["end_time"] = end_time.isoformat()
        task["result"] = result
        task["execution_time"] = (end_time - start_time).total_seconds()

        self.state["updated_at"] = datetime.now().isoformat()
        logger.info(f"[State] 任务完成: {agent_name} | 耗时: {task['execution_time']:.2f}s")

    def fail_task(self, agent_name: str, error: str) -> None:
        if agent_name not in self.state["tasks"]:
            self.start_task(agent_name)

        task = self.state["tasks"][agent_name]
        task["status"] = TaskStatus.FAILED.value
        task["error"] = error
        task["end_time"] = datetime.now().isoformat()

        self.state["updated_at"] = datetime.now().isoformat()
        logger.warning(f"[State] 任务失败: {agent_name} | error={error}")

    def retry_task(self, agent_name: str) -> None:
        if agent_name not in self.state["tasks"]:
            self.start_task(agent_name)

        task = self.state["tasks"][agent_name]
        task["status"] = TaskStatus.RETRY.value
        task["retry_count"] += 1
        task["start_time"] = datetime.now().isoformat()
        task["error"] = None

        self.state["updated_at"] = datetime.now().isoformat()
        logger.info(f"[State] 任务重试: {agent_name} | retry_count={task['retry_count']}")

    def skip_task(self, agent_name: str, reason: str) -> None:
        self.state["tasks"][agent_name] = AgentTaskRecord(
            agent_name=agent_name,
            status=TaskStatus.SKIPPED.value,
            start_time=None,
            end_time=None,
            result=None,
            error=reason,
            retry_count=0,
            execution_time=None
        )
        self.state["updated_at"] = datetime.now().isoformat()
        logger.info(f"[State] 任务跳过: {agent_name} | reason={reason}")

    def store_data(self, key: str, value: Any) -> None:
        self.state["data"][key] = value
        self.state["updated_at"] = datetime.now().isoformat()

    def get_data(self, key: str, default: Any = None) -> Any:
        return self.state["data"].get(key, default)

    def store_report(self, report_type: str, content: str) -> None:
        self.state["reports"][report_type] = content
        self.state["updated_at"] = datetime.now().isoformat()

    def get_report(self, report_type: str) -> Optional[str]:
        return self.state["reports"].get(report_type)

    def set_final_report(self, report: str) -> None:
        self.state["final_report"] = report
        self.state["updated_at"] = datetime.now().isoformat()

    def save_checkpoint(self) -> str:
        checkpoint_file = self._checkpoint_dir / f"{self.session_id}.json"
        with open(checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=2, default=str)
        logger.info(f"[State] 检查点已保存: {checkpoint_file}")
        return str(checkpoint_file)

    def load_checkpoint(self, session_id: str) -> bool:
        checkpoint_file = self._checkpoint_dir / f"{session_id}.json"
        if not checkpoint_file.exists():
            logger.warning(f"[State] 检查点文件不存在: {checkpoint_file}")
            return False

        try:
            with open(checkpoint_file, "r", encoding="utf-8") as f:
                self.state = json.load(f)
            self.session_id = session_id
            logger.info(f"[State] 检查点已加载: {checkpoint_file}")
            return True
        except Exception as e:
            logger.error(f"[State] 加载检查点失败: {e}")
            return False

    def get_task_status(self, agent_name: str) -> Optional[str]:
        task = self.state["tasks"].get(agent_name)
        return task["status"] if task else None

    def is_task_completed(self, agent_name: str) -> bool:
        return self.get_task_status(agent_name) == TaskStatus.COMPLETED.value

    def get_pending_tasks(self) -> List[str]:
        return [
            name for name, task in self.state["tasks"].items()
            if task["status"] in [TaskStatus.PENDING.value, TaskStatus.RETRY.value]
        ]

    def get_failed_tasks(self) -> List[str]:
        return [
            name for name, task in self.state["tasks"].items()
            if task["status"] == TaskStatus.FAILED.value
        ]

    def get_completed_tasks(self) -> List[str]:
        return [
            name for name, task in self.state["tasks"].items()
            if task["status"] == TaskStatus.COMPLETED.value
        ]

    def get_summary(self) -> Dict[str, Any]:
        total_tasks = len(self.state["tasks"])
        completed = len(self.get_completed_tasks())
        failed = len(self.get_failed_tasks())
        pending = len(self.get_pending_tasks())

        return {
            "stock_code": self.stock_code,
            "session_id": self.session_id,
            "phase": self.state["current_phase"],
            "progress": f"{completed}/{total_tasks}" if total_tasks > 0 else "0/0",
            "completion_rate": f"{completed/total_tasks*100:.1f}%" if total_tasks > 0 else "0%",
            "total_tasks": total_tasks,
            "completed": completed,
            "failed": failed,
            "pending": pending,
            "created_at": self.state["created_at"],
            "updated_at": self.state["updated_at"]
        }

    def reset(self) -> None:
        self.state = self._create_initial_state()
        logger.info("[State] 状态已重置")
