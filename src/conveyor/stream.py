from typing import AsyncIterable, List, TypeVar
import asyncio

T = TypeVar('T')

class AsyncStream(AsyncIterable[T]):
    def __init__(self, source: AsyncIterable[T]):
        self._source = source

    def __aiter__(self):
        return self._source.__aiter__()

    async def collect(self) -> List[T]:
        results: List[T] = []
        async for item in self._source:
            results.append(item)
        return results

    def as_completed(self):
        """
        Return an async iterator that yields results as they complete, similar to asyncio.as_completed().

        This method returns an async iterator that yields results in the order they complete,
        not in the order they were submitted. This can be more efficient when some
        items take longer to process than others.

        Note: This method currently just yields items as they come from the source.
        The real as_completed logic should happen at the pipeline/task level via execution_mode.

        Returns:
            AsyncIterable[T]: An async iterator that yields items in completion order
        """
        return self._as_completed_generator()

    async def _as_completed_generator(self) -> AsyncIterable[T]:
        """Internal generator that yields items as they complete."""
        # Since the stream items are already processed by the pipeline,
        # and the pipeline handles the execution mode internally,
        # we just yield them as they come from the source.
        async for item in self._source:
            yield item


__all__ = ["AsyncStream"]
