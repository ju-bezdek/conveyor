from typing import Callable, Iterable, List, Optional, TypeVar, Union
import functools
from .tasks import SingleTask, BatchTask
from .error_handling import ErrorAction, RetryConfig, ErrorHandler

T = TypeVar('T')

def single_task(func: Optional[Callable] = None, 
                on_error: ErrorAction = "fail",
                retry_attempts: int = 1,
                retry_delay: float = 1.0,
                retry_exponential_backoff: bool = True,
                retry_max_delay: float = 60.0,
                error_handler: Optional[ErrorHandler] = None,
                task_name: Optional[str] = None):
    """
    Decorator for creating single-item processing tasks.
    
    Args:
        func: The function to wrap
        on_error: What to do when an error occurs ("fail", "skip_item")
        retry_attempts: Maximum number of retry attempts (default: 1, no retry)
        retry_delay: Base delay between retries in seconds
        retry_exponential_backoff: Whether to use exponential backoff
        retry_max_delay: Maximum delay between retries
        error_handler: Custom error handler
        task_name: Name for logging/debugging
    """
    def decorator(f):
        retry_config = RetryConfig(
            max_attempts=retry_attempts,
            base_delay=retry_delay,
            exponential_backoff=retry_exponential_backoff,
            max_delay=retry_max_delay
        ) if retry_attempts > 1 else None

        # Check if this is a method (will be bound when accessed on an instance)
        if hasattr(f, "__self__"):
            # Already bound method
            task_func = f
        else:
            # Could be an unbound method or regular function
            # We'll handle the binding at runtime
            task_func = f

        return SingleTask(
            task_func,
            on_error=on_error,
            retry_config=retry_config,
            error_handler=error_handler,
            task_name=task_name or f.__name__,
        )

    if func is None:
        return decorator
    else:
        return decorator(func)

def batch_task(min_size: int = 1, max_size: Optional[int] = None,
               on_error: ErrorAction = "fail",
               retry_attempts: int = 1,
               retry_delay: float = 1.0,
               retry_exponential_backoff: bool = True,
               retry_max_delay: float = 60.0,
               error_handler: Optional[ErrorHandler] = None,
               task_name: Optional[str] = None):
    """
    Decorator for creating batch processing tasks.
    
    Args:
        min_size: Minimum batch size
        max_size: Maximum batch size
        on_error: What to do when an error occurs ("fail", "skip_item", "skip_batch")
        retry_attempts: Maximum number of retry attempts (default: 1, no retry)
        retry_delay: Base delay between retries in seconds
        retry_exponential_backoff: Whether to use exponential backoff
        retry_max_delay: Maximum delay between retries
        error_handler: Custom error handler
        task_name: Name for logging/debugging
    """
    def decorator(func):
        retry_config = RetryConfig(
            max_attempts=retry_attempts,
            base_delay=retry_delay,
            exponential_backoff=retry_exponential_backoff,
            max_delay=retry_max_delay
        ) if retry_attempts > 1 else None

        # Check if this is a method (will be bound when accessed on an instance)
        if hasattr(func, "__self__"):
            # Already bound method
            task_func = func
        else:
            # Could be an unbound method or regular function
            # We'll handle the binding at runtime
            task_func = func

        return BatchTask(
            task_func,
            min_size=min_size,
            max_size=max_size,
            on_error=on_error,
            retry_config=retry_config,
            error_handler=error_handler,
            task_name=task_name or func.__name__,
        )

    return decorator

__all__ = ["single_task", "batch_task"]
