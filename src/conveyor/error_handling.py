from typing import Any, Callable, Optional, Union, List, Literal
import asyncio
import logging
from dataclasses import dataclass

# Type for error handling actions
ErrorAction = Literal["fail", "skip_item", "skip_batch"]

@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    base_delay: float = 1.0  # seconds
    exponential_backoff: bool = True
    max_delay: float = 60.0  # seconds
    backoff_multiplier: float = 2.0

class ErrorHandler:
    """Base class for custom error handlers."""
    
    async def handle_error(self, error: Exception, item: Any, task_name: str, attempt: int) -> tuple[bool, Any]:
        """
        Handle an error and return (should_continue, value).
        
        Returns:
            tuple: (should_continue, value)
                - should_continue: True to continue pipeline, False to raise
                - value: Value to use (only relevant if should_continue is True)
        """
        raise NotImplementedError

class LoggingErrorHandler(ErrorHandler):
    """Error handler that logs errors and continues."""
    
    def __init__(self, logger: Optional[logging.Logger] = None, replacement_value: Any = None):
        self.logger = logger or logging.getLogger(__name__)
        self.replacement_value = replacement_value
    
    async def handle_error(self, error: Exception, item: Any, task_name: str, attempt: int) -> tuple[bool, Any]:
        self.logger.error(f"Error in {task_name} (attempt {attempt}): {error}")
        return True, self.replacement_value

__all__ = ["ErrorAction", "RetryConfig", "ErrorHandler", "LoggingErrorHandler"]