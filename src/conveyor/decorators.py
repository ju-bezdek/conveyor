from typing import Callable, Iterable, List, Optional, TypeVar, Union
from .tasks import SingleTask, BatchTask

T = TypeVar('T')

def single_task(func: Callable[[T], Union[Iterable[T], T, None]]) -> SingleTask:
    return SingleTask(func)

def batch_task(min_size: int = 1, max_size: Optional[int] = None):
    def wrapper(func: Callable[[List[T]], Union[Iterable[T], T, None]]):
        return BatchTask(func, min_size=min_size, max_size=max_size)
    return wrapper

__all__ = ["single_task", "batch_task"]