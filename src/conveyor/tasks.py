import asyncio
import inspect
from typing import Any, AsyncIterable, Callable, Iterable, List, Optional, TypeVar, Union
from contextvars import copy_context

# Forward declaration for type hinting to avoid circular import
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .pipeline import Pipeline
    from .stream import AsyncStream # Added for type hint

from .error_handling import ErrorAction, RetryConfig, ErrorHandler
from .context import get_current_context, PipelineContext

T = TypeVar('T')

UNDEFINED_VALUE = object()

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
        self._side_args = []
        self._side_kwargs = {}

    async def process(self, items: AsyncIterable[T]) -> AsyncIterable[T]:
        raise NotImplementedError

    def get_context(self) -> Optional[PipelineContext]:
        """Get the current pipeline context."""
        return get_current_context()

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

    async def as_completed(self, data):
        """Execute the task and yield results as they complete, similar to asyncio.as_completed()."""
        from .pipeline import Pipeline as PipelineClass

        pipeline = PipelineClass().add(self)
        async for item in pipeline.as_completed(data):
            yield item

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

    def __call__(self, data):
        from .pipeline import Pipeline

        return Pipeline().add(self)(data)


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
        self._instance = None  # Store the instance for bound methods

    def __get__(self, instance, owner):
        """Descriptor protocol to handle bound methods."""
        if instance is None:
            return self

        # Create a new task instance bound to the specific object instance
        bound_task = SingleTask(
            self.func,
            self._side_args,
            self._side_kwargs,
            self.on_error,
            self.retry_config,
            self.error_handler,
            self.task_name,
        )
        bound_task._instance = instance
        return bound_task

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

        # Check execution mode from context
        context = self.get_context()
        execution_mode = context.execution_mode if context else "ordered"

        if execution_mode == "as_completed":
            return await self._process_as_completed(
                items, current_resolved_args, current_resolved_kwargs
            )
        else:
            return await self._process_ordered(
                items, current_resolved_args, current_resolved_kwargs
            )

    async def _execute_single_item(
        self,
        item_to_process: Any,
        resolved_args: List[Any],
        resolved_kwargs: dict[str, Any],
    ) -> Any:
        """Shared method to execute a single item with proper error handling."""

        async def _execute():
            if item_to_process != UNDEFINED_VALUE:
                _args = (item_to_process, *resolved_args)
            else:
                _args = resolved_args

            # Handle bound methods properly
            if self._instance is not None:
                # This is a bound method, call with instance
                if inspect.isasyncgenfunction(self.func):
                    return self.func(self._instance, *_args, **resolved_kwargs)
                elif asyncio.iscoroutinefunction(self.func):
                    return await self.func(self._instance, *_args, **resolved_kwargs)
                else:
                    result = self.func(self._instance, *_args, **resolved_kwargs)
                    if inspect.isgeneratorfunction(self.func):
                        return result  # Return the generator itself
                    return result
            else:
                # Regular function or already bound method
                if inspect.isasyncgenfunction(self.func):
                    return self.func(*_args, **resolved_kwargs)
                elif asyncio.iscoroutinefunction(self.func):
                    return await self.func(*_args, **resolved_kwargs)
                else:
                    result = self.func(*_args, **resolved_kwargs)
                    if inspect.isgeneratorfunction(self.func):
                        return result  # Return the generator itself
                    return result

        return await self._execute_with_error_handling(_execute, item_to_process)

    async def _yield_result(self, result: Any) -> AsyncIterable[Any]:
        """Shared method to properly yield results based on their type."""
        if result is None:
            return  # Skip this item

        # Check if result is an async generator (from async generator function)
        if inspect.isasyncgen(result):
            async for out in result:
                yield out
        # Check if result is a regular generator (from generator function)
        elif inspect.isgenerator(result):
            for out in result:
                yield out
        # Check if result is iterable but not string/bytes
        elif isinstance(result, list):
            for out in result:
                yield out
        else:
            yield result

    async def _process_streaming_core(
        self,
        items: AsyncIterable[T],
        resolved_args: List[Any],
        resolved_kwargs: dict[str, Any],
        preserve_order: bool = True,
    ) -> AsyncIterable[Any]:
        """
        Core streaming processor that starts processing items as they arrive.

        Args:
            preserve_order: If True, yields results in input order (buffering when needed).
                          If False, yields results as they complete.
        """
        async def _gen():
            if preserve_order:
                # Ordered processing with streaming
                next_expected_index = 0
                completed_buffer = {}  # {index: result}
                pending_tasks = {}  # {index: task}
                input_finished = False
                current_index = 0

                # Create a queue to handle items as they arrive
                item_queue = asyncio.Queue()

                # Task to consume input stream and feed queue
                async def input_feeder():
                    nonlocal input_finished, current_index
                    try:
                        async for item in items:
                            await item_queue.put((current_index, item))
                            current_index += 1
                    finally:
                        input_finished = True
                        await item_queue.put(None)  # Sentinel to signal end

                # Start the input feeder
                feeder_task = asyncio.create_task(input_feeder())

                try:
                    # Process items as they arrive
                    while True:
                        # Check if we can yield any completed results in order
                        while next_expected_index in completed_buffer:
                            result = completed_buffer.pop(next_expected_index)
                            async for output in self._yield_result(result):
                                yield output
                            next_expected_index += 1

                        # If input is finished and no pending tasks and queue is empty, we're done
                        if input_finished and not pending_tasks and item_queue.empty():
                            break

                        # Wait for either a new item or a task completion
                        wait_tasks = []
                        queue_get_task = None

                        # Add item queue get if we might have more items
                        if not input_finished or not item_queue.empty():
                            queue_get_task = asyncio.create_task(item_queue.get())
                            wait_tasks.append(queue_get_task)

                        # Add pending task completions
                        if pending_tasks:
                            wait_tasks.extend(pending_tasks.values())

                        if not wait_tasks:
                            # This shouldn't happen, but break to avoid infinite loop
                            break

                        done, pending_wait_tasks = await asyncio.wait(
                            wait_tasks, return_when=asyncio.FIRST_COMPLETED
                        )

                        # Only cancel the queue get task if it wasn't completed
                        # NEVER cancel processing tasks!
                        for task in pending_wait_tasks:
                            if task == queue_get_task and not task.done():
                                task.cancel()

                        for completed_task in done:
                            # Check if this is a new item from queue
                            if queue_get_task and completed_task == queue_get_task:
                                try:
                                    queue_result = await completed_task
                                    if queue_result is None:  # Sentinel value
                                        # Input is finished, no more items
                                        continue

                                    index, item = queue_result
                                    # Start processing this item
                                    task = asyncio.create_task(
                                        self._execute_single_item(
                                            item, resolved_args, resolved_kwargs
                                        )
                                    )
                                    pending_tasks[index] = task
                                except asyncio.CancelledError:
                                    # Queue get was cancelled, that's fine
                                    pass
                            else:
                                # This is a completed processing task
                                task_index = None
                                for idx, task in pending_tasks.items():
                                    if task == completed_task:
                                        task_index = idx
                                        break

                                if task_index is not None:
                                    result = await completed_task
                                    completed_buffer[task_index] = result
                                    del pending_tasks[task_index]

                finally:
                    # Clean up
                    if not feeder_task.done():
                        feeder_task.cancel()
                        try:
                            await feeder_task
                        except asyncio.CancelledError:
                            pass

                    # Only cancel processing tasks if we're exiting due to an error
                    # Otherwise let them complete naturally
                    for task in pending_tasks.values():
                        if not task.done():
                            task.cancel()

                    # Yield any remaining results in order
                    while next_expected_index in completed_buffer:
                        result = completed_buffer.pop(next_expected_index)
                        async for output in self._yield_result(result):
                            yield output
                        next_expected_index += 1

            else:
                # As-completed processing - process items as they arrive, yield results as they complete
                pending_tasks = set()
                input_finished = False

                # Create a queue to handle items as they arrive
                item_queue = asyncio.Queue()

                # Task to consume input stream and feed queue
                async def input_feeder():
                    nonlocal input_finished
                    try:
                        async for item in items:
                            await item_queue.put(item)
                    finally:
                        input_finished = True
                        await item_queue.put(None)  # Sentinel to signal end

                # Start the input feeder
                feeder_task = asyncio.create_task(input_feeder())

                try:
                    # Process items as they arrive, yield results as they complete
                    while True:
                        # If input is finished and no pending tasks, we're done
                        if input_finished and not pending_tasks:
                            break

                        # Wait for either a new item or a task completion
                        wait_tasks = []
                        queue_get_task = None

                        # Add item queue get if we might have more items
                        if not input_finished or not item_queue.empty():
                            queue_get_task = asyncio.create_task(item_queue.get())
                            wait_tasks.append(queue_get_task)

                        # Add pending task completions
                        if pending_tasks:
                            wait_tasks.extend(pending_tasks)

                        if not wait_tasks:
                            # This shouldn't happen, but break to avoid infinite loop
                            break

                        done, pending_wait_tasks = await asyncio.wait(
                            wait_tasks, return_when=asyncio.FIRST_COMPLETED
                        )

                        # Only cancel the queue get task if it wasn't completed
                        # NEVER cancel processing tasks!
                        for task in pending_wait_tasks:
                            if task == queue_get_task and not task.done():
                                task.cancel()

                        for completed_task in done:
                            # Check if this is a new item from queue
                            if queue_get_task and completed_task == queue_get_task:
                                try:
                                    queue_result = await completed_task
                                    if queue_result is None:  # Sentinel value
                                        # Input is finished, no more items
                                        continue

                                    # Start processing this item immediately
                                    task = asyncio.create_task(
                                        self._execute_single_item(
                                            queue_result, resolved_args, resolved_kwargs
                                        )
                                    )
                                    pending_tasks.add(task)
                                except asyncio.CancelledError:
                                    # Queue get was cancelled, that's fine
                                    pass
                            else:
                                # This is a completed processing task
                                if completed_task in pending_tasks:
                                    pending_tasks.remove(completed_task)
                                    try:
                                        result = await completed_task
                                        # Yield results immediately as they complete
                                        async for output in self._yield_result(result):
                                            yield output
                                    except Exception:
                                        # Skip failed tasks - error handling should be done at task level
                                        pass

                finally:
                    # Clean up
                    if not feeder_task.done():
                        feeder_task.cancel()
                        try:
                            await feeder_task
                        except asyncio.CancelledError:
                            pass

                    # Only cancel processing tasks if we're exiting due to an error
                    # Otherwise let them complete naturally and yield their results
                    remaining_tasks = list(pending_tasks)
                    if remaining_tasks:
                        for completed_task in asyncio.as_completed(remaining_tasks):
                            try:
                                result = await completed_task
                                async for output in self._yield_result(result):
                                    yield output
                            except Exception:
                                # Skip failed tasks
                                pass

        return _gen()

    async def _process_ordered(
        self,
        items: AsyncIterable[T],
        resolved_args: List[Any],
        resolved_kwargs: dict[str, Any],
    ) -> AsyncIterable[Any]:
        """Process items preserving order while enabling streaming as soon as possible."""
        return await self._process_streaming_core(
            items, resolved_args, resolved_kwargs, preserve_order=True
        )

    async def _process_as_completed(
        self,
        items: AsyncIterable[T],
        resolved_args: List[Any],
        resolved_kwargs: dict[str, Any],
    ) -> AsyncIterable[Any]:
        """Process items yielding results as they complete."""
        return await self._process_streaming_core(
            items, resolved_args, resolved_kwargs, preserve_order=False
        )


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
        self._instance = None  # Store the instance for bound methods

    def __get__(self, instance, owner):
        """Descriptor protocol to handle bound methods."""
        if instance is None:
            return self

        # Create a new task instance bound to the specific object instance
        bound_task = BatchTask(
            self.func,
            self.min_size,
            self.max_size,
            self._side_args,
            self._side_kwargs,
            self.on_error,
            self.retry_config,
            self.error_handler,
            self.task_name,
        )
        bound_task._instance = instance
        return bound_task

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
                        # Handle bound methods properly
                        if self._instance is not None:
                            # This is a bound method, call with instance
                            if asyncio.iscoroutinefunction(self.func):
                                return await self.func(
                                    self._instance,
                                    batch_to_process,
                                    *current_resolved_args,
                                    **current_resolved_kwargs,
                                )
                            else:
                                return self.func(
                                    self._instance,
                                    batch_to_process,
                                    *current_resolved_args,
                                    **current_resolved_kwargs,
                                )
                        else:
                            # Regular function or already bound method
                            if asyncio.iscoroutinefunction(self.func):
                                return await self.func(
                                    batch_to_process,
                                    *current_resolved_args,
                                    **current_resolved_kwargs,
                                )
                            else:
                                return self.func(
                                    batch_to_process,
                                    *current_resolved_args,
                                    **current_resolved_kwargs,
                                )

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
                    # Handle bound methods properly
                    if self._instance is not None:
                        # This is a bound method, call with instance
                        if asyncio.iscoroutinefunction(self.func):
                            return await self.func(
                                self._instance,
                                buffer,
                                *current_resolved_args,
                                **current_resolved_kwargs,
                            )
                        else:
                            return self.func(
                                self._instance,
                                buffer,
                                *current_resolved_args,
                                **current_resolved_kwargs,
                            )
                    else:
                        # Regular function or already bound method
                        if asyncio.iscoroutinefunction(self.func):
                            return await self.func(
                                buffer,
                                *current_resolved_args,
                                **current_resolved_kwargs,
                            )
                        else:
                            return self.func(
                                buffer,
                                *current_resolved_args,
                                **current_resolved_kwargs,
                            )

                result = await self._execute_with_error_handling(_execute, None, buffer)

                if result is not None:
                    if isinstance(result, Iterable) and not isinstance(result, (str, bytes)):
                        for out in result:
                            yield out
                    else:
                        yield result

        return _gen()


__all__ = ["BaseTask", "SingleTask", "BatchTask"]
