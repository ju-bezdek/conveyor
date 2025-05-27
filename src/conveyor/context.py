from typing import Any, Dict, Optional, Literal
from dataclasses import dataclass, field
from contextvars import ContextVar
import asyncio

ExecutionMode = Literal["ordered", "as_completed"]

@dataclass
class PipelineContext:
    """Configuration and state for pipeline execution."""
    # Execution behavior
    execution_mode: ExecutionMode = "ordered"  # "ordered" or "as_completed"
    max_parallelism: Optional[int] = None  # None means unlimited

    # Pipeline state
    pipeline_id: Optional[str] = None

    # Custom data storage for sharing between tasks
    data: Dict[str, Any] = field(default_factory=dict)

    def copy(self) -> 'PipelineContext':
        """Create a copy of the context."""
        return PipelineContext(
            execution_mode=self.execution_mode,
            max_parallelism=self.max_parallelism,
            pipeline_id=self.pipeline_id,
            data=self.data.copy(),
        )

# Context variable for thread-local pipeline context
_pipeline_context: ContextVar[Optional[PipelineContext]] = ContextVar('pipeline_context', default=None)

def get_current_context() -> Optional[PipelineContext]:
    """Get the current pipeline context."""
    return _pipeline_context.get()

def set_current_context(context: PipelineContext) -> None:
    """Set the current pipeline context."""
    _pipeline_context.set(context)

class ContextManager:
    """Context manager for pipeline execution."""
    
    def __init__(self, context: PipelineContext):
        self.context = context
        self.previous_context = None
    
    def __enter__(self):
        self.previous_context = get_current_context()
        set_current_context(self.context)
        return self.context
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        set_current_context(self.previous_context)

async def with_context(context: PipelineContext, coro):
    """Run a coroutine with a specific pipeline context."""
    with ContextManager(context):
        return await coro

__all__ = ["PipelineContext", "ExecutionMode", "get_current_context", "set_current_context", "ContextManager", "with_context"]
