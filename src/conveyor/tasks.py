import asyncio
from typing import Any, AsyncIterable, Callable, Iterable, List, Optional, TypeVar, Union
# Forward declaration for type hinting to avoid circular import
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .pipeline import Pipeline
    from .stream import AsyncStream # Added for type hint

from .error_handling import ErrorAction, RetryConfig, ErrorHandler

T = TypeVar('T')

class BaseTask:
    def __init__(self, 
                 on_error: ErrorAction = "fail",
                 retry_config: Optional[RetryConfig] = None,
                 error_handler: Optional[ErrorHandler] = None,
                 task_name: Optional[str] = None):
        self.on_error = on_error
        self.retry_config = retry_config or RetryConfig(max_attempts=1)  # No retry by default
        self.error_handler = error_handler
        self.task_name = task_name or self.__class__.__name__

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

    def __call__(self, data) -> 'AsyncStream':
        """Make individual tasks callable by wrapping them in a pipeline."""
        from .pipeline import Pipeline as PipelineClass
        from .stream import AsyncStream
        pipeline = PipelineClass().add(self)
        return pipeline(data)

    async def _execute_with_error_handling(self, func_call: Callable, item: Any, batch: Optional[List[Any]] = None):
        """Execute a function call with error handling and retry logic."""
        last_error = None
        
        for attempt in range(1, self.retry_config.max_attempts + 1):
            try:
                return await func_call()
            except Exception as error:
                last_error = error
                
                # If we have more attempts, calculate delay and continue
                if attempt < self.retry_config.max_attempts:
                    delay = self._calculate_retry_delay(attempt)
                    await asyncio.sleep(delay)
                    continue
                
                # Last attempt failed, handle the error
                return await self._handle_final_error(error, item, batch, attempt)
        
        # Should never reach here, but just in case
        raise last_error

    async def _handle_final_error(self, error: Exception, item: Any, batch: Optional[List[Any]], attempt: int):
        """Handle error after all retry attempts are exhausted."""
        
        # Custom error handler takes precedence
        if self.error_handler:
            should_continue, value = await self.error_handler.handle_error(error, item, self.task_name, attempt)
            if should_continue:
                return value
            else:
                raise error
        
        # Built-in error actions
        if self.on_error == "fail":
            raise error
        elif self.on_error == "skip_item":
            return None  # Signal to skip this item
        elif self.on_error == "skip_batch":
            return None  # Signal to skip batch
        else:
            raise error

    def _calculate_retry_delay(self, attempt: int) -> float:
        """Calculate delay for retry attempt."""
        if not self.retry_config.exponential_backoff:
            return self.retry_config.base_delay
        
        delay = self.retry_config.base_delay * (
            self.retry_config.backoff_multiplier ** (attempt - 1)
        )
        return min(delay, self.retry_config.max_delay)

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
                 _side_kwargs: Optional[dict[str, Any]] = None,
                 on_error: ErrorAction = "fail",
                 retry_config: Optional[RetryConfig] = None,
                 error_handler: Optional[ErrorHandler] = None,
                 task_name: Optional[str] = None):
        super().__init__(on_error, retry_config, error_handler, task_name)
        self.func = func
        self._side_args = _side_args or []
        self._side_kwargs = _side_kwargs or {}
        self._resolved_side_values: Optional[tuple[List[Any], dict[str, Any]]] = None

    def with_inputs(self, *args: Any, **kwargs: Any) -> 'SingleTask':
        """Returns a new SingleTask instance configured with side inputs."""
        return SingleTask(
            self.func, 
            _side_args=list(args), 
            _side_kwargs=kwargs,
            on_error=self.on_error,
            retry_config=self.retry_config,
            error_handler=self.error_handler,
            task_name=self.task_name
        )

    async def process(self, items: AsyncIterable[T]) -> AsyncIterable[Any]:
        if self._resolved_side_values is None:
            resolved_args = [await self._resolve_side_input(arg) for arg in self._side_args]
            resolved_kwargs = {k: await self._resolve_side_input(v) for k, v in self._side_kwargs.items()}
            self._resolved_side_values = (resolved_args, resolved_kwargs)
        
        current_resolved_args, current_resolved_kwargs = self._resolved_side_values

        async def _gen():
            async for item in items:
                async def _execute():
                    if asyncio.iscoroutinefunction(self.func):
                        return await self.func(item, *current_resolved_args, **current_resolved_kwargs)
                    else:
                        return self.func(item, *current_resolved_args, **current_resolved_kwargs)
                
                result = await self._execute_with_error_handling(_execute, item)
                
                if result is None:
                    continue  # Skip this item
                
                if isinstance(result, Iterable) and not isinstance(result, (str, bytes)):
                    for out in result:
                        yield out
                else:
                    yield result
        return _gen()

class BatchTask(BaseTask):
    def __init__(self, func: Callable[..., Union[Iterable[Any], Any, None]],
                 min_size: int = 1, max_size: Optional[int] = None,
                 _side_args: Optional[List[Any]] = None,
                 _side_kwargs: Optional[dict[str, Any]] = None,
                 on_error: ErrorAction = "fail",
                 retry_config: Optional[RetryConfig] = None,
                 error_handler: Optional[ErrorHandler] = None,
                 task_name: Optional[str] = None):
        super().__init__(on_error, retry_config, error_handler, task_name)
        self.func = func
        self.min_size = min_size
        self.max_size = max_size or min_size
        self._side_args = _side_args or []
        self._side_kwargs = _side_kwargs or {}
        self._resolved_side_values: Optional[tuple[List[Any], dict[str, Any]]] = None

    def with_inputs(self, *args: Any, **kwargs: Any) -> 'BatchTask':
        """Returns a new BatchTask instance configured with side inputs."""
        return BatchTask(
            self.func, 
            self.min_size, 
            self.max_size, 
            _side_args=list(args), 
            _side_kwargs=kwargs,
            on_error=self.on_error,
            retry_config=self.retry_config,
            error_handler=self.error_handler,
            task_name=self.task_name
        )

    async def process(self, items: AsyncIterable[T]) -> AsyncIterable[Any]:
        if self._resolved_side_values is None:
            resolved_args = [await self._resolve_side_input(arg) for arg in self._side_args]
            resolved_kwargs = {k: await self._resolve_side_input(v) for k, v in self._side_kwargs.items()}
            self._resolved_side_values = (resolved_args, resolved_kwargs)

        current_resolved_args, current_resolved_kwargs = self._resolved_side_values
        buffer: List[T] = []

        async def _gen():
            nonlocal buffer
            async for item in items:
                buffer.append(item)
                while self.max_size and len(buffer) >= self.max_size:
                    batch_to_process, buffer = buffer[:self.max_size], buffer[self.max_size:]
                    
                    async def _execute():
                        if asyncio.iscoroutinefunction(self.func):
                            return await self.func(batch_to_process, *current_resolved_args, **current_resolved_kwargs)
                        else:
                            return self.func(batch_to_process, *current_resolved_args, **current_resolved_kwargs)
                    
                    result = await self._execute_with_error_handling(_execute, None, batch_to_process)
                    
                    if result is None:
                        continue  # Skip this batch
                    
                    if isinstance(result, Iterable) and not isinstance(result, (str, bytes)):
                        for out in result:
                            yield out
                    else:
                        yield result
            
            # Process remaining buffer
            if buffer and len(buffer) >= self.min_size:
                async def _execute():
                    if asyncio.iscoroutinefunction(self.func):
                        return await self.func(buffer, *current_resolved_args, **current_resolved_kwargs)
                    else:
                        return self.func(buffer, *current_resolved_args, **current_resolved_kwargs)
                
                result = await self._execute_with_error_handling(_execute, None, buffer)
                
                if result is not None:
                    if isinstance(result, Iterable) and not isinstance(result, (str, bytes)):
                        for out in result:
                            yield out
                    else:
                        yield result
        
        return _gen()

__all__ = ["BaseTask", "SingleTask", "BatchTask"]