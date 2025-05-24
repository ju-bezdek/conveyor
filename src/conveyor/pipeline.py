from re import U
from typing import Any, AsyncIterable, Iterable, List, TypeVar, Union, Optional
from .stream import AsyncStream
from .tasks import BaseTask, UNDEFINED_VALUE
from .context import PipelineContext, ContextManager, ExecutionMode
import uuid

T = TypeVar('T')


# Used to represent undefined argument

class Pipeline:

    def __init__(self, context: Optional[PipelineContext] = None):
        self.stages: List[BaseTask] = []
        self.context = context or PipelineContext()

    def add(self, *tasks: BaseTask) -> 'Pipeline':
        self.stages.extend(tasks)
        return self

    def with_context(self, **kwargs) -> "Pipeline":
        """Create a new pipeline with modified context settings."""
        new_context = self.context.copy()
        for key, value in kwargs.items():
            if hasattr(new_context, key):
                setattr(new_context, key, value)
            else:
                new_context.data[key] = value
        # Create new pipeline and copy stages from current pipeline
        new_pipeline = Pipeline(new_context)
        new_pipeline.stages = self.stages.copy()
        return new_pipeline

    def with_execution_mode(self, mode: ExecutionMode) -> "Pipeline":
        """Set the execution mode for this pipeline."""
        return self.with_context(execution_mode=mode)

    def __or__(self, other: Union[BaseTask, 'Pipeline']) -> 'Pipeline':
        if isinstance(other, BaseTask):
            return Pipeline(self.context.copy()).add(*self.stages).add(other)
        if isinstance(other, Pipeline): # Check against Pipeline class itself
            new_context = self.context.copy()
            new_context.stage_count = len(self.stages) + len(other.stages)
            return Pipeline(new_context).add(*self.stages, *other.stages)
        raise TypeError(f"Cannot pipe Pipeline to {type(other)}")

    def __call__(
        self, data: Iterable[T] = UNDEFINED_VALUE, *args, **kwargs
    ) -> AsyncStream[T]:
        if not (isinstance(data, Iterable) and not isinstance(data, (str, tuple))):
            # if first argument is not an iterable we consider it as intial arg for first stage
            if args:
                args = (data, *args)
            elif data != UNDEFINED_VALUE:
                args = (data,)
            data = UNDEFINED_VALUE

        stream = AsyncStream(self._run_pipeline(data, *args, **kwargs))
        # The execution mode is handled through the context system in _run_pipeline
        # No need to call stream.as_completed() here
        return stream

    async def as_completed(self, data: Iterable[T]) -> AsyncIterable[T]:
        """
        Execute the pipeline and yield results as they complete, similar to asyncio.as_completed().

        Args:
            data: Input data to process through the pipeline

        Yields:
            Results as they complete, without preserving input order
        """
        # Create a pipeline configured for as_completed execution
        as_completed_pipeline = self.with_execution_mode("as_completed")
        async for item in as_completed_pipeline(data):
            yield item

    def _run_pipeline(
        self, data: Iterable[T] = UNDEFINED_VALUE, *args, **kwargs
    ) -> AsyncIterable[T]:
        async def gen():
            # Set up context for this pipeline execution
            execution_context = self.context.copy()
            execution_context.pipeline_id = str(uuid.uuid4())
            execution_context.stage_count = len(self.stages)

            # Use the context manager for the entire pipeline execution
            with ContextManager(execution_context):
                current_stream: AsyncIterable[Any] = self._make_input_async(data)
                for stage_index, stage in enumerate(self.stages):
                    # Update context for current stage
                    execution_context.current_stage = stage_index
                    if stage_index == 0 and (args or kwargs):
                        # If there are kwargs, pass them to the first stage
                        if args:
                            stage._side_args = args  # args provided in pipeline call override any side_args defined in the stage
                        if kwargs:
                            stage._side_kwargs.update(**kwargs)

                    # The process method is async and returns an AsyncIterable
                    # Pass the context explicitly to ensure it's available in all task executions
                    current_stream = await stage.process(current_stream)

                async for item in current_stream:
                    yield item
        return gen()

    def _make_input_async(
        self, data: Iterable[T] = UNDEFINED_VALUE
    ) -> AsyncIterable[T]:
        async def _gen():
            if isinstance(data, AsyncIterable):
                # If the input data is already an AsyncIterable, return it directly
                async for item in data:
                    yield item
            elif isinstance(data, list):
                for item in data:
                    yield item
            else:
                yield data
        return _gen()


__all__ = ["Pipeline"]
