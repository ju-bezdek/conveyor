\
import pytest
import asyncio
from conveyor.stream import AsyncStream

async def simple_async_generator(data):
    for item in data:
        await asyncio.sleep(0.001) # Simulate async work
        yield item

@pytest.mark.asyncio
async def test_async_stream_collect():
    data = [1, 2, 3, 4, 5]
    source_gen = simple_async_generator(data)
    stream = AsyncStream(source_gen)
    result = await stream.collect()
    assert result == data

@pytest.mark.asyncio
async def test_async_stream_aiter():
    data = [10, 20, 30]
    source_gen = simple_async_generator(data)
    stream = AsyncStream(source_gen)
    
    collected_items = []
    async for item in stream:
        collected_items.append(item)
    assert collected_items == data

@pytest.mark.asyncio
async def test_async_stream_collect_empty():
    data = []
    source_gen = simple_async_generator(data)
    stream = AsyncStream(source_gen)
    result = await stream.collect()
    assert result == data

@pytest.mark.asyncio
async def test_async_stream_aiter_empty():
    data = []
    source_gen = simple_async_generator(data)
    stream = AsyncStream(source_gen)
    
    collected_items = []
    async for item in stream:
        collected_items.append(item) # Should not run
    assert collected_items == data

# Test that AsyncStream can be iterated multiple times if the underlying source can be
# Note: Our simple_async_generator is a generator, so it will be exhausted after one iteration.
# For re-iteration, the source itself would need to support it, or AsyncStream would need to cache.
# This test demonstrates the current behavior (exhaustion).
@pytest.mark.asyncio
async def test_async_stream_iteration_exhaustion():
    data = [1, 2]
    source_gen = simple_async_generator(data)
    stream = AsyncStream(source_gen)

    first_pass = []
    async for item in stream:
        first_pass.append(item)
    assert first_pass == data

    second_pass = []
    async for item in stream: # Generator is exhausted
        second_pass.append(item)
    assert second_pass == []
