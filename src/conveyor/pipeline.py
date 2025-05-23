from typing import Any, AsyncIterable, Iterable, List, TypeVar, Union
from .stream import AsyncStream
from .tasks import BaseTask

T = TypeVar('T')

class Pipeline:
    def __init__(self):
        self.stages: List[BaseTask] = []

    def add(self, *tasks: BaseTask) -> 'Pipeline':
        self.stages.extend(tasks)
        return self

    def __or__(self, other: Union[BaseTask, 'Pipeline']) -> 'Pipeline':
        if isinstance(other, BaseTask):
            return Pipeline().add(*self.stages).add(other)
        if isinstance(other, Pipeline): # Check against Pipeline class itself
            return Pipeline().add(*self.stages, *other.stages)
        raise TypeError(f"Cannot pipe Pipeline to {type(other)}")

    def __call__(self, data: Iterable[T]) -> AsyncStream[T]:
        return AsyncStream(self._run_pipeline(data))

    def _run_pipeline(self, data: Iterable[T]) -> AsyncIterable[T]:
        async def gen():
            current_stream: AsyncIterable[Any] = self._make_input_async(data)
            for stage in self.stages:
                # The process method is async and returns an AsyncIterable
                current_stream = await stage.process(current_stream)
            async for item in current_stream:
                yield item
        return gen()

    def _make_input_async(self, data: Iterable[T]) -> AsyncIterable[T]:
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
