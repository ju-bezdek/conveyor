\
import pytest
import asyncio
from conveyor.tasks import SingleTask, BatchTask
from conveyor.stream import AsyncStream # For creating test inputs

async def async_gen_from_list(data):
    for item in data:
        yield item

@pytest.mark.asyncio
async def test_single_task_sync_func():
    def multiply(x):
        return x * 2
    task = SingleTask(multiply)
    input_data = [1, 2, 3]
    stream = AsyncStream(async_gen_from_list(input_data))
    
    results = []
    async for item in await task.process(stream): # MODIFIED
        results.append(item)
    assert results == [2, 4, 6]

@pytest.mark.asyncio
async def test_single_task_async_func():
    async def multiply_async(x):
        await asyncio.sleep(0.001)
        return x * 3
    task = SingleTask(multiply_async)
    input_data = [1, 2, 3]
    stream = AsyncStream(async_gen_from_list(input_data))
    
    results = []
    async for item in await task.process(stream): # MODIFIED
        results.append(item)
    assert results == [3, 6, 9]

@pytest.mark.asyncio
async def test_single_task_func_returns_iterable():
    def expand(x):
        return [x, x+1]
    task = SingleTask(expand)
    input_data = [1, 3]
    stream = AsyncStream(async_gen_from_list(input_data))
    
    results = []
    async for item in await task.process(stream): # MODIFIED
        results.append(item)
    assert results == [1, 2, 3, 4]

@pytest.mark.asyncio
async def test_single_task_func_returns_none():
    def filter_even(x):
        if x % 2 == 0:
            return x
        return None
    task = SingleTask(filter_even)
    input_data = [1, 2, 3, 4, 5]
    stream = AsyncStream(async_gen_from_list(input_data))
    
    results = []
    async for item in await task.process(stream): # MODIFIED
        results.append(item)
    assert results == [2, 4]

@pytest.mark.asyncio
async def test_batch_task_sync_func():
    def sum_batch(batch):
        return sum(batch)
    task = BatchTask(sum_batch, min_size=2, max_size=3)
    input_data = [1, 2, 3, 4, 5, 6, 7] # Batches: [1,2,3], [4,5,6], remainder [7] (processed as min_size is 1 by default if not flushed)
                                      # With min_size=2, [7] is not processed
    stream = AsyncStream(async_gen_from_list(input_data))
    
    results = []
    async for item in await task.process(stream): # MODIFIED
        results.append(item)
    # Batch 1: 1+2+3 = 6
    # Batch 2: 4+5+6 = 15
    # Remainder [7] - len is 1, min_size is 2, so it's not processed by default flush.
    assert results == [6, 15]


@pytest.mark.asyncio
async def test_batch_task_async_func():
    async def sum_batch_async(batch):
        await asyncio.sleep(0.001)
        return sum(batch)
    task = BatchTask(sum_batch_async, min_size=2, max_size=2)
    input_data = [1, 2, 3, 4, 5] # Batches: [1,2], [3,4], remainder [5]
    stream = AsyncStream(async_gen_from_list(input_data))
    
    results = []
    async for item in await task.process(stream): # MODIFIED
        results.append(item)
    # Batch 1: 1+2 = 3
    # Batch 2: 3+4 = 7
    # Remainder [5] - len is 1, min_size is 2. Not processed.
    assert results == [3, 7]

@pytest.mark.asyncio
async def test_batch_task_returns_iterable():
    def sum_and_double_batch(batch):
        s = sum(batch)
        return [s, s * 2]
    task = BatchTask(sum_and_double_batch, min_size=2, max_size=2)
    input_data = [1, 2, 3, 4] # Batches: [1,2], [3,4]
    stream = AsyncStream(async_gen_from_list(input_data))
    
    results = []
    async for item in await task.process(stream): # MODIFIED
        results.append(item)
    # Batch 1: sum=3 -> [3, 6]
    # Batch 2: sum=7 -> [7, 14]
    assert results == [3, 6, 7, 14]

@pytest.mark.asyncio
async def test_batch_task_returns_none():
    def sum_if_positive_batch(batch):
        s = sum(batch)
        if s > 0:
            return s
        return None
    task = BatchTask(sum_if_positive_batch, min_size=1, max_size=2)
    input_data = [1, 2, -5, 1, 3, -10] # Batches: [1,2], [-5,1], [3,-10]
    stream = AsyncStream(async_gen_from_list(input_data))
    
    results = []
    async for item in await task.process(stream): # MODIFIED
        results.append(item)
    # Batch 1: 1+2=3 -> 3
    # Batch 2: -5+1=-4 -> None
    # Batch 3: [3] (buffer)
    # Batch 4: [-10] (buffer becomes [3,-10]) -> sum -7 -> yields None
    # If input is [1, 2, -5, 1, 3, -10]
    # Process [1,2] -> sum 3. Yield 3. Buffer empty.
    # Process [-5,1] -> sum -4. Yield None. Buffer empty.
    # Process [3]. Buffer [3].
    # Process [-10]. Buffer [3,-10]. Max size hit. Process [3,-10]. Sum -7. Yield None. Buffer empty.
    # Final flush: buffer is empty.
    assert results == [3]


@pytest.mark.asyncio
async def test_batch_task_remainder_processing_gt_min_size():
    def sum_batch(batch):
        return sum(batch)
    # min_size=2, max_size=3. Remainder of size 2 or 3 should be processed.
    task = BatchTask(sum_batch, min_size=2, max_size=3)
    input_data = [1, 2, 3, 4, 5, 6, 7, 8] # Batches: [1,2,3], [4,5,6], remainder [7,8]
    stream = AsyncStream(async_gen_from_list(input_data))
    
    results = []
    async for item in await task.process(stream): # MODIFIED
        results.append(item)
    # Batch 1: 1+2+3 = 6
    # Batch 2: 4+5+6 = 15
    # Remainder [7,8] - len is 2, min_size is 2. Processed. 7+8 = 15
    assert results == [6, 15, 15]

@pytest.mark.asyncio
async def test_batch_task_remainder_processing_eq_min_size():
    def sum_batch(batch):
        return sum(batch)
    task = BatchTask(sum_batch, min_size=2, max_size=3)
    input_data = [1,2,3,4,5] # Batch [1,2,3], Remainder [4,5]
    stream = AsyncStream(async_gen_from_list(input_data))
    results = []
    async for item in await task.process(stream): results.append(item) # MODIFIED
    assert results == [6, 9]


@pytest.mark.asyncio
async def test_batch_task_remainder_processing_lt_min_size():
    def sum_batch(batch):
        return sum(batch)
    # min_size=2, max_size=3. Remainder of size 1 should NOT be processed.
    task = BatchTask(sum_batch, min_size=2, max_size=3)
    input_data = [1, 2, 3, 4, 5, 6, 7] # Batches: [1,2,3], [4,5,6], remainder [7]
    stream = AsyncStream(async_gen_from_list(input_data))
    
    results = []
    async for item in await task.process(stream): # MODIFIED
        results.append(item)
    # Batch 1: 1+2+3 = 6
    # Batch 2: 4+5+6 = 15
    # Remainder [7] - len is 1, min_size is 2. Not processed.
    assert results == [6, 15]

@pytest.mark.asyncio
async def test_batch_task_exact_batch_size_no_remainder():
    def sum_batch(batch):
        return sum(batch)
    task = BatchTask(sum_batch, min_size=2, max_size=2)
    input_data = [1, 2, 3, 4] # Batches: [1,2], [3,4]
    stream = AsyncStream(async_gen_from_list(input_data))
    
    results = []
    async for item in await task.process(stream): # MODIFIED
        results.append(item)
    assert results == [3, 7]

@pytest.mark.asyncio
async def test_batch_task_empty_input():
    def sum_batch(batch):
        return sum(batch) # Should not be called
    task = BatchTask(sum_batch, min_size=1, max_size=2)
    input_data = []
    stream = AsyncStream(async_gen_from_list(input_data))
    
    results = []
    async for item in await task.process(stream): # MODIFIED
        results.append(item)
    assert results == []

@pytest.mark.asyncio
async def test_batch_task_min_size_one_processes_all_remainders():
    def identity(batch): # Returns the batch itself
        return batch 
    task = BatchTask(identity, min_size=1, max_size=3)
    input_data = [1,2,3,4,5] # Batches: [1,2,3], then [4,5]
    stream = AsyncStream(async_gen_from_list(input_data))
    results = []
    # The task.process returns an async generator of items, not batches
    # and BatchTask's func can return an iterable or a single item.
    # If func returns an iterable, its elements are yielded one by one.
    # So if identity returns [1,2,3], then 1, 2, 3 are yielded.
    # If identity returns [4,5], then 4, 5 are yielded.
    async for item in await task.process(stream): # MODIFIED
        results.append(item)
    assert results == [1,2,3,4,5]
