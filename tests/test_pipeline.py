import pytest
import asyncio
import time
from conveyor.pipeline import Pipeline
from conveyor.tasks import SingleTask, BatchTask
from conveyor.decorators import single_task, batch_task
from conveyor.stream import AsyncStream # For type hinting and direct use if needed

# Helper to create a simple async generator for pipeline input
async def async_input_generator(data):
    for item in data:
        await asyncio.sleep(0.001) # simulate async nature
        yield item

# --- Define some simple tasks for testing ---
@single_task
def multiply_by_two_sync(x: int) -> int:
    # print(f"multiply_by_two_sync processing: {x}")
    return x * 2

@single_task
async def add_one_async(x: int) -> int:
    # print(f"add_one_async processing: {x}")
    await asyncio.sleep(0.001)
    return x + 1

@batch_task(min_size=2, max_size=3)
def sum_batch_sync(batch: list[int]) -> int:
    # print(f"sum_batch_sync processing: {batch}")
    s = sum(batch)
    # print(f"sum_batch_sync result: {s}")
    return s

@batch_task(min_size=2, max_size=2)
async def product_batch_async(batch: list[int]) -> int:
    # print(f"product_batch_async processing: {batch}")
    await asyncio.sleep(0.001)
    p = 1
    for x in batch:
        p *= x
    # print(f"product_batch_async result: {p}")
    return p

@single_task
def to_str_sync(x: int) -> str:
    return str(x)

# --- Test Cases ---

@pytest.mark.asyncio
async def test_pipeline_empty():
    pipeline = Pipeline()
    input_data = [1, 2, 3]
    stream = pipeline(input_data) # Calls _run_pipeline with _make_input_async
    
    results = await stream.collect()
    assert results == input_data # Empty pipeline should just pass through data

@pytest.mark.asyncio
async def test_pipeline_single_sync_task():
    pipeline = Pipeline().add(multiply_by_two_sync)
    input_data = [1, 2, 3]
    stream = pipeline(input_data)
    results = await stream.collect()
    assert results == [2, 4, 6]

@pytest.mark.asyncio
async def test_pipeline_single_async_task():
    pipeline = Pipeline().add(add_one_async)
    input_data = [10, 20]
    stream = pipeline(input_data)
    results = await stream.collect()
    assert results == [11, 21]

@pytest.mark.asyncio
async def test_pipeline_multiple_single_tasks():
    pipeline = Pipeline().add(multiply_by_two_sync).add(add_one_async) # or multiply_by_two_sync | add_one_async
    input_data = [1, 2, 3] # (1*2)+1=3, (2*2)+1=5, (3*2)+1=7
    stream = pipeline(input_data)
    results = await stream.collect()
    assert results == [3, 5, 7]

@pytest.mark.asyncio
async def test_pipeline_with_sync_batch_task():
    # multiply -> sum_batch (size 2-3)
    # Input: 1, 2, 3, 4, 5
    # multiply_by_two_sync: 2, 4, 6, 8, 10
    # sum_batch_sync (min=2, max=3):
    #   Batch 1: [2, 4, 6] -> sum = 12
    #   Batch 2: [8, 10] -> sum = 18 (remainder, processed as len >= min_size)
    pipeline = multiply_by_two_sync | sum_batch_sync
    input_data = [1, 2, 3, 4, 5]
    stream = pipeline(input_data)
    results = await stream.collect()
    assert results == [12, 18]

@pytest.mark.asyncio
async def test_pipeline_with_async_batch_task():
    # add_one_async -> product_batch_async (size 2)
    # Input: 1, 2, 3, 4
    # add_one_async: 2, 3, 4, 5
    # product_batch_async (min=2, max=2):
    #   Batch 1: [2, 3] -> product = 6
    #   Batch 2: [4, 5] -> product = 20
    pipeline = add_one_async | product_batch_async
    input_data = [1, 2, 3, 4]
    stream = pipeline(input_data)
    results = await stream.collect()
    assert results == [6, 20]

@pytest.mark.asyncio
async def test_pipeline_complex_chain():
    # Input: 1, 2, 3, 4, 5, 6
    # 1. multiply_by_two_sync: 2, 4, 6, 8, 10, 12
    # 2. sum_batch_sync (min=2, max=3):
    #    Batch [2,4,6] -> 12
    #    Batch [8,10,12] -> 30
    #    Output: 12, 30
    # 3. add_one_async: 13, 31
    # 4. to_str_sync: "13", "31"
    pipeline = multiply_by_two_sync | sum_batch_sync | add_one_async | to_str_sync
    input_data = [1, 2, 3, 4, 5, 6]
    stream = pipeline(input_data)
    results = await stream.collect()
    assert results == ["13", "31"]

@pytest.mark.asyncio
async def test_pipeline_operator_or():
    pipeline1 = multiply_by_two_sync | add_one_async
    pipeline2 = sum_batch_sync | to_str_sync
    full_pipeline = pipeline1 | pipeline2 
    # This means: multiply -> add_one -> sum_batch -> to_str

    # Input: 1, 2, 3, 4, 5, 6
    # 1. multiply_by_two_sync: 2, 4, 6, 8, 10, 12
    # 2. add_one_async: 3, 5, 7, 9, 11, 13
    # 3. sum_batch_sync (min=2, max=3):
    #    Batch [3,5,7] -> 15
    #    Batch [9,11,13] -> 33
    #    Output: 15, 33
    # 4. to_str_sync: "15", "33"
    input_data = [1, 2, 3, 4, 5, 6]
    stream = full_pipeline(input_data)
    results = await stream.collect()
    assert results == ["15", "33"]
    
    # Test piping a task to a pipeline
    pipeline3 = multiply_by_two_sync | (sum_batch_sync | add_one_async)
    # Input: 1,2,3,4,5
    # mult: 2,4,6,8,10
    # sum_batch: [2,4,6]->12, [8,10]->18. Output: 12, 18
    # add_one: 13, 19
    stream3 = pipeline3([1,2,3,4,5])
    results3 = await stream3.collect()
    assert results3 == [13,19]

    # Test piping a pipeline to a task
    pipeline4 = (multiply_by_two_sync | sum_batch_sync) | add_one_async
    # Input: 1,2,3,4,5
    # mult: 2,4,6,8,10
    # sum_batch: [2,4,6]->12, [8,10]->18. Output: 12, 18
    # add_one: 13, 19
    stream4 = pipeline4([1,2,3,4,5])
    results4 = await stream4.collect()
    assert results4 == [13,19]


@pytest.mark.asyncio
async def test_pipeline_async_iteration():
    pipeline = multiply_by_two_sync | add_one_async
    input_data = [1, 5, 10]
    stream = pipeline(input_data)
    
    expected = [(1*2)+1, (5*2)+1, (10*2)+1] # [3, 11, 21]
    collected_results = []
    async for item in stream:
        collected_results.append(item)
    assert collected_results == expected

@pytest.mark.asyncio
async def test_pipeline_batch_task_flushing_remainder():
    # Test case where the total number of items is not a multiple of batch_size
    # sum_batch_sync has min_size=2, max_size=3
    pipeline = Pipeline().add(sum_batch_sync) # MODIFIED: Wrap task in a Pipeline
    
    # Case 1: Remainder meets min_size
    input_data_1 = [1,2,3,4,5] # Batch [1,2,3] -> 6. Remainder [4,5] (len 2 >= min_size 2) -> 9
    stream1 = pipeline(input_data_1)
    results1 = await stream1.collect()
    assert results1 == [6, 9]

    # Case 2: Remainder less than min_size
    input_data_2 = [1,2,3,4] # Batch [1,2,3] -> 6. Remainder [4] (len 1 < min_size 2) -> not processed
    stream2 = pipeline(input_data_2)
    results2 = await stream2.collect()
    assert results2 == [6]

    # Case 3: No remainder
    input_data_3 = [1,2,3,4,5,6] # Batch [1,2,3] -> 6. Batch [4,5,6] -> 15
    stream3 = pipeline(input_data_3)
    results3 = await stream3.collect()
    assert results3 == [6, 15]
    
    # Case 4: Input less than min_size
    input_data_4 = [1] # Remainder [1] (len 1 < min_size 2) -> not processed
    stream4 = pipeline(input_data_4)
    results4 = await stream4.collect()
    assert results4 == []

@pytest.mark.asyncio
async def test_pipeline_task_returning_iterable():
    @single_task
    def expand_item(x):
        return [x, x*10, x*100]

    pipeline = expand_item | add_one_async
    # Input: 1, 2
    # expand_item: 1, 10, 100,   2, 20, 200
    # add_one_async: 2, 11, 101,   3, 21, 201
    input_data = [1,2]
    stream = pipeline(input_data)
    results = await stream.collect()
    assert results == [2, 11, 101, 3, 21, 201]

@pytest.mark.asyncio
async def test_pipeline_task_returning_none():
    @single_task
    def filter_even(x):
        if x % 2 == 0:
            return x
        return None # This item should be dropped

    pipeline = filter_even | multiply_by_two_sync
    # Input: 1, 2, 3, 4, 5
    # filter_even: None, 2, None, 4, None -> effectively 2, 4
    # multiply_by_two_sync: 4, 8
    input_data = [1,2,3,4,5]
    stream = pipeline(input_data)
    results = await stream.collect()
    assert results == [4, 8]


# --- Parallelism Tests ---


@pytest.mark.asyncio
async def test_pipeline_single_task_parallelism():
    """
    Test that single tasks in a pipeline process items in parallel.

    With parallel processing:
    - Stage 1: All items processed concurrently (1s total)
    - Stage 2: All items processed concurrently (1s total)
    - Total: ~2s

    With sequential processing:
    - Stage 1: N items × 1s each = N seconds
    - Stage 2: N items × 1s each = N seconds
    - Total: 2N seconds
    """

    @single_task
    async def slow_task_1(x: int) -> int:
        """First stage - 1 second sleep per item"""
        await asyncio.sleep(1.0)
        return x * 2

    @single_task
    async def slow_task_2(x: int) -> int:
        """Second stage - 1 second sleep per item"""
        await asyncio.sleep(1.0)
        return x + 10

    pipeline = slow_task_1 | slow_task_2
    input_data = [1, 2, 3, 4, 5]  # 5 items

    start_time = time.time()
    stream = pipeline(input_data)
    results = await stream.collect()
    end_time = time.time()

    total_time = end_time - start_time

    # Verify results are correct
    expected = [(x * 2) + 10 for x in input_data]  # [12, 14, 16, 18, 20]
    assert results == expected

    # Verify timing - should be around 2s (parallel), not 10s (sequential)
    print(f"\nParallelism test - Single tasks: {total_time:.2f}s")
    assert (
        total_time < 3.0
    ), f"Pipeline took {total_time:.2f}s, expected < 3s (parallel processing)"
    assert (
        total_time >= 1.9
    ), f"Pipeline took {total_time:.2f}s, expected >= 1.9s (realistic timing)"


@pytest.mark.asyncio
async def test_pipeline_single_task_parallelism_larger_dataset():
    """
    Test parallelism with a larger dataset to make timing differences more apparent.
    """

    @single_task
    async def slow_multiply(x: int) -> int:
        await asyncio.sleep(0.5)  # 500ms per item
        return x * 3

    @single_task
    async def slow_add(x: int) -> int:
        await asyncio.sleep(0.5)  # 500ms per item
        return x + 100

    pipeline = slow_multiply | slow_add
    input_data = list(range(1, 11))  # 10 items

    start_time = time.time()
    stream = pipeline(input_data)
    results = await stream.collect()
    end_time = time.time()

    total_time = end_time - start_time

    # Verify results
    expected = [(x * 3) + 100 for x in input_data]
    assert results == expected

    # With 10 items and 0.5s per stage:
    # Parallel: ~1s total (both stages overlap)
    # Sequential: 10s total (5s + 5s)
    print(f"\nParallelism test - Large dataset: {total_time:.2f}s")
    assert (
        total_time < 2.0
    ), f"Pipeline took {total_time:.2f}s, expected < 2s (parallel)"
    assert total_time >= 0.9, f"Pipeline took {total_time:.2f}s, expected >= 0.9s"


@pytest.mark.asyncio
async def test_pipeline_batch_task_parallelism():
    """
    Test that batch tasks process batches in parallel when possible.

    With max_size=3 and 6 items:
    - Batch 1: [1,2,3] - processes in 1s
    - Batch 2: [4,5,6] - processes in 1s (can start while stage 2 processes batch 1)

    Expected timeline:
    T=0s: Stage 1 starts batch [1,2,3]
    T=1s: Stage 1 finishes batch [1,2,3], starts batch [4,5,6]
          Stage 2 starts processing result from batch [1,2,3]
    T=2s: Stage 1 finishes batch [4,5,6]
          Stage 2 finishes first batch, starts second batch
    T=3s: Stage 2 finishes second batch

    Total: ~3s (with parallelism between stages)
    Sequential: 4s (2s for stage 1 + 2s for stage 2)
    """

    @batch_task(min_size=1, max_size=3)
    async def slow_batch_sum(batch: list[int]) -> int:
        """Stage 1 - sum batch with 1s delay"""
        await asyncio.sleep(1.0)
        return sum(batch)

    @single_task
    async def slow_multiply_result(x: int) -> int:
        """Stage 2 - multiply result with 1s delay"""
        await asyncio.sleep(1.0)
        return x * 10

    pipeline = slow_batch_sum | slow_multiply_result
    input_data = [1, 2, 3, 4, 5, 6]  # Will create 2 batches of 3

    start_time = time.time()
    stream = pipeline(input_data)
    results = await stream.collect()
    end_time = time.time()

    total_time = end_time - start_time

    # Verify results
    # Batch 1: sum([1,2,3]) = 6 -> 6 * 10 = 60
    # Batch 2: sum([4,5,6]) = 15 -> 15 * 10 = 150
    assert results == [60, 150]

    # Should take around 3s with pipeline parallelism, not 4s sequential
    print(f"\nParallelism test - Batch tasks: {total_time:.2f}s")
    assert (
        total_time < 3.5
    ), f"Pipeline took {total_time:.2f}s, expected < 3.5s (parallel batch processing)"
    assert total_time >= 2.9, f"Pipeline took {total_time:.2f}s, expected >= 2.9s"


@pytest.mark.asyncio
async def test_pipeline_batch_task_throttling():
    """
    Test batch task with smaller max_size to verify throttling behavior.

    With max_size=2 and 6 items, we get 3 batches.
    Each batch takes 1s to process, so stage 1 takes 3s total.
    Stage 2 processes each result as it comes (1s each), so also 3s.
    With pipelining, total should be around 4s, not 6s.
    """

    @batch_task(min_size=1, max_size=2)
    async def slow_batch_multiply(batch: list[int]) -> int:
        """Stage 1 - multiply all in batch with 1s delay"""
        await asyncio.sleep(1.0)
        result = 1
        for x in batch:
            result *= x
        return result

    @single_task
    async def slow_add_hundred(x: int) -> int:
        """Stage 2 - add 100 with 1s delay"""
        await asyncio.sleep(1.0)
        return x + 100

    pipeline = slow_batch_multiply | slow_add_hundred
    input_data = [1, 2, 3, 4, 5, 6]  # Will create 3 batches of 2

    start_time = time.time()
    stream = pipeline(input_data)
    results = await stream.collect()
    end_time = time.time()

    total_time = end_time - start_time

    # Verify results
    # Batch 1: 1*2 = 2 -> 2 + 100 = 102
    # Batch 2: 3*4 = 12 -> 12 + 100 = 112
    # Batch 3: 5*6 = 30 -> 30 + 100 = 130
    assert results == [102, 112, 130]

    # Timeline with pipelining:
    # T=0: Start batch 1
    # T=1: Finish batch 1, start batch 2, start processing result 1
    # T=2: Finish batch 2, start batch 3, finish result 1, start result 2
    # T=3: Finish batch 3, finish result 2, start result 3
    # T=4: Finish result 3
    # Total: ~4s with pipelining vs 6s sequential
    print(f"\nParallelism test - Batch throttling: {total_time:.2f}s")
    assert (
        total_time < 4.5
    ), f"Pipeline took {total_time:.2f}s, expected < 4.5s (throttled but pipelined)"
    assert total_time >= 3.9, f"Pipeline took {total_time:.2f}s, expected >= 3.9s"


@pytest.mark.asyncio
async def test_pipeline_mixed_parallelism():
    """
    Test a complex pipeline with both single and batch tasks to verify
    end-to-end parallelism.
    """

    @single_task
    async def slow_increment(x: int) -> int:
        """Stage 1 - increment with delay"""
        await asyncio.sleep(0.5)
        return x + 1

    @batch_task(min_size=1, max_size=2)
    async def slow_batch_sum(batch: list[int]) -> int:
        """Stage 2 - sum batch with delay"""
        await asyncio.sleep(0.5)
        return sum(batch)

    @single_task
    async def slow_square(x: int) -> int:
        """Stage 3 - square with delay"""
        await asyncio.sleep(0.5)
        return x * x

    pipeline = slow_increment | slow_batch_sum | slow_square
    input_data = [1, 2, 3, 4]  # 4 items

    start_time = time.time()
    stream = pipeline(input_data)
    results = await stream.collect()
    end_time = time.time()

    total_time = end_time - start_time

    # Verify results
    # Stage 1: [2, 3, 4, 5] (parallel, 0.5s total)
    # Stage 2: [5, 9] (batches [2,3] and [4,5], each 0.5s, total 1s)
    # Stage 3: [25, 81] (parallel, 0.5s total)
    # With pipelining: should be around 1.5-2s instead of 2s sequential
    assert results == [25, 81]

    print(f"\nParallelism test - Mixed pipeline: {total_time:.2f}s")
    assert (
        total_time < 2.5
    ), f"Pipeline took {total_time:.2f}s, expected < 2.5s (mixed parallelism)"
    assert total_time >= 1.4, f"Pipeline took {total_time:.2f}s, expected >= 1.4s"
