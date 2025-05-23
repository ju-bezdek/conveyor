from typing import AsyncIterable, List, TypeVar

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

__all__ = ["AsyncStream"]