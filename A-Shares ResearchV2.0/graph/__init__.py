"""[DEPRECATED] LangGraph 工作流包

⚠️ 此包已废弃，请使用 layers.agents.chief_agent.ChiefAgent 作为唯一编排入口。
   保留仅用于回滚参考，将在后续版本移除。
"""
import warnings

warnings.warn(
    "graph 包已废弃，请使用 layers.agents.chief_agent.ChiefAgent。"
    "本包将在后续版本移除。",
    DeprecationWarning,
    stacklevel=2
)

from graph.workflow import create_workflow, run_workflow, AgentState

__all__ = [
    "create_workflow",
    "run_workflow",
    "AgentState"
]
