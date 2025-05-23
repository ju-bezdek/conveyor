import asyncio
from typing import Any, AsyncIterable, Callable, Iterable, List, Optional, TypeVar, Union
# Forward declaration for type hinting to avoid circular import
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .pipeline import Pipeline
    from .stream import AsyncStream # Added for type hint

T = TypeVar('T')

class BaseTask:
    async def process(self, items: AsyncIterable[T]) -> AsyncIterable[T]:
        raise NotImplementedError

    def __or__(self, other: Union['BaseTask', 'Pipeline']) -> 'Pipeline':
        from .pipeline import Pipeline as PipelineClass  # Local import to avoid circular dependency
        if isinstance(other, BaseTask):
            return PipelineClass().add(self).add(other)
        if isinstance(other, PipelineClass):
            # Correctly add self to the beginning of the other pipeline's stages
            new_pipeline = PipelineClass()
            new_pipeline.stages.append(self)
            new_pipeline.stages.extend(other.stages)
            return new_pipeline
        raise TypeError(f"Cannot pipe {type(self)} to {type(other)}")

    # Helper method to resolve a single side input.
    async def _resolve_side_input(self, side_input: Any) -> Any:
        from .stream import AsyncStream # Local import for type check
        if isinstance(side_input, AsyncStream):
            # Policy: take the first item from the AsyncStream.
            # This is suitable if the stream is expected to provide a single value.
            async for first_item in side_input:
                return first_item
            return None # Or raise an error if an item was expected
        elif asyncio.iscoroutine(side_input):
            return await side_input
        return side_input

class SingleTask(BaseTask):
    def __init__(self, func: Callable[..., Union[Iterable[Any], Any, None]], 
                 _side_args: Optional[List[Any]] = None, 
                 _side_kwargs: Optional[dict[str, Any]] = None):
        self.func = func
        self._side_args = _side_args or []
        self._side_kwargs = _side_kwargs or {}
        self._resolved_side_values: Optional[tuple[List[Any], dict[str, Any]]] = None

    def with_inputs(self, *args: Any, **kwargs: Any) -> 'SingleTask':
        """Returns a new SingleTask instance configured with side inputs."""
        return SingleTask(self.func, _side_args=list(args), _side_kwargs=kwargs)

    async def process(self, items: AsyncIterable[T]) -> AsyncIterable[Any]:
        if self._resolved_side_values is None:
            resolved_args = [await self._resolve_side_input(arg) for arg in self._side_args]
            resolved_kwargs = {k: await self._resolve_side_input(v) for k, v in self._side_kwargs.items()}
            self._resolved_side_values = (resolved_args, resolved_kwargs)
        
        current_resolved_args, current_resolved_kwargs = self._resolved_side_values

        async def _gen():
            async for item in items:
                if asyncio.iscoroutinefunction(self.func):
                    result = await self.func(item, *current_resolved_args, **current_resolved_kwargs)
                else:
                    result = self.func(item, *current_resolved_args, **current_resolved_kwargs)
                
                if result is None:
                    continue
                if isinstance(result, Iterable) and not isinstance(result, (str, bytes)):
                    for out in result:
                        yield out
                else:
                    yield result # type: ignore
        return _gen()

class BatchTask(BaseTask):
    def __init__(self, func: Callable[..., Union[Iterable[Any], Any, None]],
                 min_size: int = 1, max_size: Optional[int] = None,
                 _side_args: Optional[List[Any]] = None,
                 _side_kwargs: Optional[dict[str, Any]] = None):
        self.func = func
        self.min_size = min_size
        self.max_size = max_size or min_size
        self._side_args = _side_args or []
        self._side_kwargs = _side_kwargs or {}
        self._resolved_side_values: Optional[tuple[List[Any], dict[str, Any]]] = None

    def with_inputs(self, *args: Any, **kwargs: Any) -> 'BatchTask':
        """Returns a new BatchTask instance configured with side inputs."""
        return BatchTask(self.func, self.min_size, self.max_size, 
                         _side_args=list(args), _side_kwargs=kwargs)

    async def process(self, items: AsyncIterable[T]) -> AsyncIterable[Any]:
        if self._resolved_side_values is None:
            resolved_args = [await self._resolve_side_input(arg) for arg in self._side_args]
            resolved_kwargs = {k: await self._resolve_side_input(v) for k, v in self._side_kwargs.items()}
            self._resolved_side_values = (resolved_args, resolved_kwargs)

        current_resolved_args, current_resolved_kwargs = self._resolved_side_values
        buffer: List[T] = []

        async def _execute_func(batch_or_item: Union[List[T], T]):
            if asyncio.iscoroutinefunction(self.func):
                return await self.func(batch_or_item, *current_resolved_args, **current_resolved_kwargs)
            else:
                return self.func(batch_or_item, *current_resolved_args, **current_resolved_kwargs)

        async def _gen():
            nonlocal buffer
            async for item in items:
                buffer.append(item)
                while self.max_size and len(buffer) >= self.max_size:
                    batch_to_process, buffer = buffer[:self.max_size], buffer[self.max_size:]
                    result = await _execute_func(batch_to_process)

                    if result is None:
                        continue
                    if isinstance(result, Iterable) and not isinstance(result, (str, bytes)):
                        for out in result: yield out
                    else:
                        yield result # type: ignore
            
            if buffer and len(buffer) >= self.min_size:
                result = await _execute_func(buffer)
                if result is not None:
                    if isinstance(result, Iterable) and not isinstance(result, (str, bytes)):
                        for out in result: yield out
                    else:
                        yield result # type: ignore
        return _gen()

__all__ = ["BaseTask", "SingleTask", "BatchTask"]