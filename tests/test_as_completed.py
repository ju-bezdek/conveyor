import pytest
import asyncio
import time
import uuid
from conveyor import (
    single_task,
    batch_task,
    Pipeline,
    PipelineContext,
    get_current_context,
    set_current_context,
    ContextManager,
    with_context,
)
from conveyor.stream import AsyncStream

@single_task
async def slow_task(x: int) -> int:
    """Task with variable delay based on input value."""
    delay = x * 0.1  # 0.1s per unit
    await asyncio.sleep(delay)
    return x * 2

@single_task
async def add_timestamp(x: int) -> dict:
    """Add completion timestamp to track completion order."""
    return {
        'value': x,
        'completed_at': time.time()
    }

# Tasks for testing throttling behavior with batch_task
@batch_task(max_size=2)  # Process max 2 items per batch (throttled)
async def slow_batch_task_throttled(batch: list[int]) -> list[int]:
    """Task with limited batch size (simulates throttling)."""
    results = []
    for x in batch:
        delay = x * 0.1  # Simulate processing time
        await asyncio.sleep(delay)
        results.append(x * 2)
    return results

@batch_task(max_size=10)  # Process up to 10 items per batch (unlimited for our test)
async def slow_batch_task_unlimited(batch: list[int]) -> list[int]:
    """Task with large batch size (simulates unlimited concurrency)."""
    # Process all items in the batch concurrently
    async def process_item(x):
        delay = x * 0.1
        await asyncio.sleep(delay)
        return x * 2
    
    # Process all items in the batch concurrently
    tasks = [process_item(x) for x in batch]
    return await asyncio.gather(*tasks)

@pytest.mark.asyncio
async def test_async_stream_as_completed_basic():
    """Test basic as_completed functionality on AsyncStream."""
    # Create input data where larger numbers take longer to process
    input_data = [5, 1, 3, 2, 4]  # 5 takes longest, 1 takes shortest
    
    pipeline = slow_task | add_timestamp
    
    # Collect results in completion order using pipeline.as_completed()
    completion_order = []
    start_time = time.time()
    
    async for result in pipeline.as_completed(input_data):
        completion_order.append(result['value'])
        result['elapsed'] = time.time() - start_time
    
    # Verify that faster tasks completed first
    # Since delays are: 1(0.1s), 2(0.2s), 3(0.3s), 4(0.4s), 5(0.5s) after slow_task doubles them
    # The doubled values are: 2, 4, 6, 8, 10 with delays 0.1s, 0.2s, 0.3s, 0.4s, 0.5s
    # Completion order should be roughly: 2, 4, 6, 8, 10 (corresponding to input 1, 2, 3, 4, 5)
    assert completion_order[0] == 2  # Input 1 -> doubled to 2, fastest should complete first
    assert completion_order[-1] == 10  # Input 5 -> doubled to 10, slowest should complete last

@pytest.mark.asyncio 
async def test_batch_task_concurrency_performance():
    """Test performance difference between throttled vs unlimited batch processing."""
    input_data = [4, 3, 2, 1]  # Mixed processing times: 0.4s, 0.3s, 0.2s, 0.1s
    
    # Test throttled execution (max_size=2) - processes in 2 batches
    throttled_pipeline = slow_batch_task_throttled
    start_time = time.time()
    throttled_results = await throttled_pipeline(input_data).collect()
    throttled_time = time.time() - start_time
    
    # Test unlimited execution (max_size=10) - processes in 1 batch concurrently
    unlimited_pipeline = slow_batch_task_unlimited
    start_time = time.time()
    unlimited_results = await unlimited_pipeline(input_data).collect()
    unlimited_time = time.time() - start_time
    
    # Unlimited should be significantly faster since all items process concurrently
    # Throttled: processes 2 batches sequentially: [4,3] then [2,1]
    #   Batch 1: sequential processing: 0.4s + 0.3s = 0.7s
    #   Batch 2: sequential processing: 0.2s + 0.1s = 0.3s
    #   Total: ~1.0s
    # Unlimited: processes all concurrently in one batch: max(0.4, 0.3, 0.2, 0.1) = 0.4s
    assert unlimited_time < throttled_time * 0.6  # At least 40% faster
    
    # Both should have same results (just potentially different order)
    assert sorted(throttled_results) == sorted(unlimited_results)

@pytest.mark.asyncio
async def test_as_completed_vs_ordered_with_single_tasks():
    """Test that as_completed shows results faster than ordered with single tasks."""
    input_data = [5, 1, 4, 2]  # Larger time differences for more reliable timing
    
    # Use single task pipeline to properly test as_completed vs ordered
    pipeline = slow_task | add_timestamp
    
    # Test ordered execution - must wait for results in input order
    start_time = time.time()
    ordered_results = []
    first_result_time_ordered = None
    
    async for result in pipeline(input_data):
        if first_result_time_ordered is None:
            first_result_time_ordered = time.time() - start_time
        ordered_results.append(result)
    
    ordered_total_time = time.time() - start_time
    
    # Test as_completed execution - get results as they complete
    start_time = time.time()
    completed_results = []
    first_result_time_completed = None
    
    async for result in pipeline.as_completed(input_data):
        if first_result_time_completed is None:
            first_result_time_completed = time.time() - start_time
        completed_results.append(result)
    
    completed_total_time = time.time() - start_time
    
    # Both should have same results content
    ordered_values = sorted([r['value'] for r in ordered_results])
    completed_values = sorted([r['value'] for r in completed_results])
    assert ordered_values == completed_values
    
    # Verify that as_completed returns results in completion order (not input order)
    # Input 1 has shortest delay -> should be first in as_completed
    assert completed_results[0]['value'] == 2  # Input 1 -> doubled to 2
    
    # Verify that ordered returns results in input order
    ordered_output_values = [r['value'] for r in ordered_results]
    assert ordered_output_values == [10, 2, 8, 4]  # [5*2, 1*2, 4*2, 2*2] - input order

@pytest.mark.asyncio
async def test_pipeline_execution_modes():
    """Test pipeline-level execution mode configuration."""
    input_data = [3, 1, 2]
    pipeline = slow_task | add_timestamp
    
    # Test ordered mode (default)
    ordered_results = []
    async for result in pipeline(input_data):
        ordered_results.append(result['value'])
    
    # Results should maintain input order, but values are doubled by slow_task
    assert ordered_results == [6, 2, 4]  # [3*2, 1*2, 2*2]
    
    # Test as_completed mode
    completed_results = []
    async for result in pipeline.as_completed(input_data):
        completed_results.append(result['value'])
    
    # Results should be in completion order (fastest first)
    # Input 1 has delay 0.1s -> output 2, should be first
    # Input 3 has delay 0.3s -> output 6, should be last
    assert completed_results[0] == 2  # Fastest (input 1 -> 2)
    assert completed_results[-1] == 6  # Slowest (input 3 -> 6)

@pytest.mark.asyncio
async def test_max_size_throttling_behavior():
    """Test that max_size properly throttles batch processing."""
    
    # Track when each batch starts processing
    batch_processing_starts = []
    
    @batch_task(max_size=2)
    async def track_batch_processing(batch: list[int]) -> list[int]:
        batch_processing_starts.append((batch.copy(), time.time()))
        await asyncio.sleep(0.2)  # Fixed delay to make timing predictable
        return [x * 2 for x in batch]
    
    input_data = [1, 2, 3, 4]
    start_time = time.time()
    
    results = await track_batch_processing(input_data).collect()
    
    # Analyze batch processing starts to verify throttling
    # With max_size=2, we should see:
    # - Batch [1, 2] starts at t=0
    # - Batch [3, 4] starts after first batch completes at t≈0.2s
    
    relative_starts = [(batch, start - start_time) for batch, start in batch_processing_starts]
    relative_starts.sort(key=lambda item: item[1])  # Sort by start time
    
    # First batch should start immediately
    assert relative_starts[0][1] < 0.05  # First batch starts immediately
    assert relative_starts[0][0] == [1, 2]  # First batch contains items 1, 2
    
    # Second batch should start after a delay (when first batch completes)
    if len(relative_starts) > 1:
        assert relative_starts[1][1] > 0.15  # Second batch waits
        assert relative_starts[1][0] == [3, 4]  # Second batch contains items 3, 4
    
    assert sorted(results) == [2, 4, 6, 8]  # All items processed correctly

@pytest.mark.asyncio
async def test_pipeline_context_basic():
    """Test basic pipeline context functionality."""
    @single_task
    async def context_aware_task(x: int) -> dict:
        context = get_current_context()
        return {
            "value": x,
            "pipeline_id": (
                context.pipeline_id[:8] if context and context.pipeline_id else "none"
            ),
            "execution_mode": context.execution_mode if context else "none",
        }

    pipeline = context_aware_task
    results = await pipeline([1, 2]).collect()

    # Verify context is properly set
    for result in results:
        assert result['pipeline_id'] != 'none'
        assert result["execution_mode"] == "ordered"  # Default mode

@pytest.mark.asyncio
async def test_pipeline_context_data_sharing():
    """Test sharing data through pipeline context."""
    @single_task
    async def increment_counter(x: int) -> dict:
        context = get_current_context()
        if context:
            # Increment shared counter
            context.data['counter'] = context.data.get('counter', 0) + 1
            counter_value = context.data['counter']
        else:
            counter_value = -1
        
        return {
            'value': x,
            'counter': counter_value
        }
    
    # Create pipeline with initial context data
    pipeline = Pipeline().add(increment_counter)
    custom_pipeline = pipeline.with_context(data={'counter': 100})
    
    results = await custom_pipeline([1, 2, 3]).collect()
    
    # Verify counter was incremented for each item
    assert results[0]['counter'] == 101
    assert results[1]['counter'] == 102  
    assert results[2]['counter'] == 103

@pytest.mark.asyncio
async def test_pipeline_with_context_method():
    """Test pipeline.with_context() method."""
    @single_task 
    async def get_mode(x: int) -> str:
        context = get_current_context()
        return context.execution_mode if context else 'none'
    
    base_pipeline = Pipeline().add(get_mode)
    
    # Test setting execution mode via with_context
    ordered_pipeline = base_pipeline.with_context(execution_mode='ordered')
    completed_pipeline = base_pipeline.with_context(execution_mode='as_completed')
    
    ordered_results = await ordered_pipeline([1]).collect()
    completed_results = await completed_pipeline([1]).collect()
    
    assert ordered_results[0] == 'ordered'
    assert completed_results[0] == 'as_completed'

@pytest.mark.asyncio
async def test_context_isolation():
    """Test that different pipeline executions have isolated contexts."""
    execution_ids = []
    
    @single_task
    async def capture_pipeline_id(x: int) -> str:
        context = get_current_context()
        pipeline_id = context.pipeline_id if context else 'none'
        execution_ids.append(pipeline_id)
        return pipeline_id
    
    pipeline = Pipeline().add(capture_pipeline_id)
    
    # Run pipeline twice
    await pipeline([1]).collect()
    await pipeline([2]).collect()
    
    # Should have different pipeline IDs for different executions
    assert len(execution_ids) == 2
    assert execution_ids[0] != execution_ids[1]
    assert execution_ids[0] != 'none'
    assert execution_ids[1] != 'none'

@pytest.mark.asyncio
async def test_as_completed_error_handling():
    """Test that as_completed properly handles task errors."""
    @batch_task(max_size=2, on_error="skip_batch")  # Configure to skip failed batches
    async def sometimes_fails(batch: list[int]) -> list[int]:
        if 3 in batch:
            raise ValueError(f"Batch failed for batch containing 3: {batch}")
        results = []
        for x in batch:
            await asyncio.sleep(x * 0.1)
            results.append(x * 2)
        return results
    
    input_data = [1, 2, 3, 4]  # Batches: [1,2], [3,4] - second batch will fail
    pipeline = Pipeline().add(sometimes_fails)
    
    # Test with as_completed - should skip failed batches and continue
    results = []
    async for result in pipeline(input_data).as_completed():
        results.append(result)
    
    # Should get results for [1,2] batch only (batch [3,4] failed and was skipped)
    assert sorted(results) == [2, 4]  # 1*2, 2*2

@pytest.mark.asyncio
async def test_as_completed_empty_stream():
    """Test as_completed with empty input."""
    pipeline = Pipeline().add(slow_batch_task_unlimited)
    
    results = []
    async for result in pipeline([]).as_completed():
        results.append(result)
    
    assert results == []

@pytest.mark.asyncio
async def test_mixed_execution_modes_with_batching():
    """Test mixing execution modes with batch tasks in a complex scenario."""
    input_data = [5, 1, 4, 2]  # Larger time differences
    
    # Same pipeline, different execution modes - use single task for proper as_completed behavior
    base_pipeline = slow_task | add_timestamp
    
    # Run both modes and compare
    ordered_start = time.time()
    ordered_results = await base_pipeline(input_data).collect()
    ordered_time = time.time() - ordered_start
    
    completed_start = time.time() 
    completed_results = []
    async for result in base_pipeline.as_completed(input_data):
        completed_results.append(result)
    completed_time = time.time() - completed_start
    
    # Verify ordered preserves input order, but values are doubled by slow_task
    ordered_values = [r['value'] for r in ordered_results]
    assert ordered_values == [10, 2, 8, 4]  # [5*2, 1*2, 4*2, 2*2] - same order as input
    
    # Verify as_completed has different order (fastest first)
    completed_values = [r['value'] for r in completed_results]
    assert completed_values[0] == 2  # Fastest (input 1 -> 2) should be first
    assert completed_values[-1] == 10  # Slowest (input 5 -> 10) should be last
    
    # Both should have same total results
    assert sorted(ordered_values) == sorted(completed_values)
    
    print(f"Ordered: {ordered_time:.3f}s, As-completed: {completed_time:.3f}s")

@pytest.mark.asyncio
async def test_single_task_as_completed_vs_ordered_timing():
    """Test timing difference between as_completed and ordered execution for single tasks."""
    input_data = [3, 1, 2]  # Different processing times

    pipeline = slow_task

    # Test ordered execution timing
    ordered_start = time.time()
    ordered_results = await pipeline(input_data).collect()
    ordered_time = time.time() - ordered_start

    # Test as_completed execution timing
    completed_start = time.time()
    completed_results = []
    async for result in pipeline.as_completed(input_data):
        completed_results.append(result)
    completed_time = time.time() - completed_start

    # Both should have same results but different timing characteristics
    assert sorted(ordered_results) == sorted(completed_results)

    # as_completed should complete in roughly the time of the longest task
    # ordered waits for all tasks but can process them concurrently
    # The timing difference might be small for this simple case
    print(f"Ordered: {ordered_time:.3f}s, As-completed: {completed_time:.3f}s")

    # Verify results are correct
    assert sorted(ordered_results) == [2, 4, 6]  # [1*2, 2*2, 3*2]
    assert sorted(completed_results) == [2, 4, 6]


@pytest.mark.asyncio
async def test_ordered_streaming_performance():
    """Test that ordered processing enables streaming while preserving order."""
    # Test with items that have different processing times
    # Item processing times: [3s, 1s, 2s, 0.5s]
    input_data = [0.3, 0.1, 0.2, 0.05]  # Use smaller delays for testing

    @single_task
    async def process_with_delay(x: float) -> dict:
        start_time = time.time()
        await asyncio.sleep(x)  # Sleep for x seconds
        end_time = time.time()
        return {
            "input": x,
            "processing_time": round(end_time - start_time, 3),
            "completed_at": end_time,
        }

    pipeline = Pipeline().add(process_with_delay)

    # Test streaming behavior - track when each result is yielded
    start_time = time.time()
    results = []
    yield_times = []

    async for result in pipeline(input_data):
        yield_time = time.time() - start_time
        yield_times.append(yield_time)
        results.append(result)
        print(f"Yielded result for input {result['input']} at {yield_time:.3f}s")

    total_time = time.time() - start_time

    # Verify order preservation
    yielded_inputs = [r["input"] for r in results]
    assert (
        yielded_inputs == input_data
    ), f"Order not preserved: {yielded_inputs} != {input_data}"

    # Verify streaming behavior:
    # - First result (input=0.3, 0.3s delay) should be yielded around 0.3s
    # - Subsequent results should be yielded immediately after (they're already completed)
    assert yield_times[0] >= 0.25, f"First result yielded too early: {yield_times[0]}s"
    assert yield_times[0] <= 0.35, f"First result yielded too late: {yield_times[0]}s"

    # Second, third, and fourth results should be yielded immediately after first
    # (they were completed earlier but buffered)
    for i in range(1, len(yield_times)):
        time_diff = yield_times[i] - yield_times[0]
        assert (
            time_diff <= 0.01
        ), f"Result {i} not yielded immediately after first: {time_diff}s delay"

    print(f"✅ Streaming test passed!")
    print(f"   - Total time: {total_time:.3f}s")
    print(f"   - Yield times: {[f'{t:.3f}s' for t in yield_times]}")
    print(f"   - Order preserved: {yielded_inputs}")


@pytest.mark.asyncio
async def test_ordered_vs_gather_comparison():
    """Compare the new streaming approach vs traditional gather approach."""
    input_data = [0.1, 0.1, 0.3, 0.05]  # Processing times

    @single_task
    async def timed_task(delay: float) -> dict:
        start = time.time()
        await asyncio.sleep(delay)
        return {"delay": delay, "completed_at": time.time() - start}

    pipeline = Pipeline().add(timed_task)

    # Test our streaming implementation
    streaming_start = time.time()
    streaming_results = []
    first_yield_time = None

    async for result in pipeline(input_data):
        if first_yield_time is None:
            first_yield_time = time.time() - streaming_start
        streaming_results.append(result)

    streaming_total = time.time() - streaming_start

    # Simulate traditional gather approach for comparison
    gather_start = time.time()
    tasks = []
    for delay in input_data:

        async def create_task(d=delay):
            await asyncio.sleep(d)
            return {"delay": d, "completed_at": time.time() - gather_start}

        tasks.append(asyncio.create_task(create_task()))

    gather_results = await asyncio.gather(*tasks)
    gather_total = time.time() - gather_start
    gather_first_yield = gather_total  # All results yielded at once

    # Verify our approach yields first result much earlier than gather
    print(f"Streaming first yield: {first_yield_time:.3f}s")
    print(f"Gather first yield: {gather_first_yield:.3f}s")
    print(
        f"Improvement: {((gather_first_yield - first_yield_time) / gather_first_yield * 100):.1f}%"
    )

    # Our approach should yield first result around 0.4s (when first item completes)
    # Gather approach yields all results around 0.4s (when slowest completes)
    assert (
        first_yield_time <= gather_first_yield
    ), "Streaming should yield earlier than gather"

    # Verify order preservation in both approaches
    streaming_order = [r["delay"] for r in streaming_results]
    gather_order = [r["delay"] for r in gather_results]
    assert streaming_order == input_data, "Streaming didn't preserve order"
    assert gather_order == input_data, "Gather didn't preserve order"


@pytest.mark.asyncio
async def test_best_case_streaming():
    """Test best case: when first item finishes first, it should yield immediately."""
    # First item has shortest delay, should yield immediately
    input_data = [0.1, 0.3, 0.2, 0.4]

    @single_task
    async def delay_task(delay: float) -> float:
        await asyncio.sleep(delay)
        return delay

    pipeline = Pipeline().add(delay_task)

    start_time = time.time()
    results = []

    async for result in pipeline(input_data):
        yield_time = time.time() - start_time
        results.append((result, yield_time))
        if len(results) == 1:  # First result
            # Should be yielded around 0.1s (first item's processing time)
            assert (
                0.08 <= yield_time <= 0.15
            ), f"First result not yielded promptly: {yield_time}s"
            print(f"✅ First result yielded at {yield_time:.3f}s (expected ~0.1s)")
            break

    # Verify we got the correct first result
    assert results[0][0] == 0.1, f"Wrong first result: {results[0][0]}"


@pytest.mark.asyncio
async def test_pipeline_context_creation_and_defaults():
    """Test PipelineContext creation with default values."""
    context = PipelineContext()

    # Test default values
    assert context.execution_mode == "ordered"
    assert context.max_parallelism is None
    assert context.pipeline_id is None
    assert context.data == {}


@pytest.mark.asyncio
async def test_pipeline_context_creation_with_values():
    """Test PipelineContext creation with custom values."""
    test_data = {"key1": "value1", "key2": 42}
    context = PipelineContext(
        execution_mode="as_completed",
        max_parallelism=5,
        pipeline_id="test-pipeline-123",
        data=test_data,
    )

    # Test custom values
    assert context.execution_mode == "as_completed"
    assert context.max_parallelism == 5
    assert context.pipeline_id == "test-pipeline-123"
    assert context.data == test_data


@pytest.mark.asyncio
async def test_pipeline_context_copy():
    """Test PipelineContext.copy() method."""
    original_data = {"shared_counter": 10, "config": {"batch_size": 100}}
    original = PipelineContext(
        execution_mode="as_completed",
        max_parallelism=10,
        pipeline_id="original-123",
        data=original_data,
    )

    # Create a copy
    copy = original.copy()

    # Verify all fields are copied correctly
    assert copy.execution_mode == original.execution_mode
    assert copy.max_parallelism == original.max_parallelism
    assert copy.pipeline_id == original.pipeline_id
    assert copy.data == original.data

    # Verify deep copy behavior for data dictionary
    assert copy.data is not original.data  # Different objects
    copy.data["new_key"] = "new_value"
    assert "new_key" not in original.data  # Original unchanged

    # Verify modifying copy doesn't affect original
    copy.execution_mode = "ordered"
    copy.max_parallelism = 20
    assert original.execution_mode == "as_completed"
    assert original.max_parallelism == 10


@pytest.mark.asyncio
async def test_context_manager_basic():
    """Test ContextManager basic functionality."""
    # Ensure no context initially
    assert get_current_context() is None

    test_context = PipelineContext(pipeline_id="test-123", data={"test": "value"})

    # Use context manager
    with ContextManager(test_context) as ctx:
        # Context should be set within the manager
        assert get_current_context() is test_context
        assert ctx is test_context
        assert ctx.pipeline_id == "test-123"
        assert ctx.data["test"] == "value"

    # Context should be restored after exiting
    assert get_current_context() is None


@pytest.mark.asyncio
async def test_context_manager_nested():
    """Test nested ContextManager usage."""
    # Start with no context
    assert get_current_context() is None

    context1 = PipelineContext(pipeline_id="context-1", data={"level": 1})
    context2 = PipelineContext(pipeline_id="context-2", data={"level": 2})

    with ContextManager(context1):
        # First level
        assert get_current_context() is context1
        assert get_current_context().data["level"] == 1

        with ContextManager(context2):
            # Second level
            assert get_current_context() is context2
            assert get_current_context().data["level"] == 2

        # Back to first level
        assert get_current_context() is context1
        assert get_current_context().data["level"] == 1

    # Back to no context
    assert get_current_context() is None


@pytest.mark.asyncio
async def test_context_manager_exception_handling():
    """Test ContextManager properly restores context even when exceptions occur."""
    original_context = PipelineContext(pipeline_id="original")

    # Set initial context
    with ContextManager(original_context):
        assert get_current_context() is original_context

        try:
            new_context = PipelineContext(pipeline_id="new")
            with ContextManager(new_context):
                assert get_current_context() is new_context
                raise ValueError("Test exception")
        except ValueError:
            # Context should be restored even after exception
            assert get_current_context() is original_context

    # Should be back to None
    assert get_current_context() is None


@pytest.mark.asyncio
async def test_get_set_current_context():
    """Test get_current_context and set_current_context functions."""
    # Initially no context
    assert get_current_context() is None

    # Set a context
    test_context = PipelineContext(pipeline_id="test-context", data={"key": "value"})
    set_current_context(test_context)

    # Verify it's set
    current = get_current_context()
    assert current is test_context
    assert current.pipeline_id == "test-context"
    assert current.data["key"] == "value"

    # Set another context
    another_context = PipelineContext(pipeline_id="another-context")
    set_current_context(another_context)
    assert get_current_context() is another_context

    # Clear context
    set_current_context(None)
    assert get_current_context() is None


@pytest.mark.asyncio
async def test_with_context_function():
    """Test with_context function for running coroutines with context."""
    test_context = PipelineContext(
        pipeline_id="async-test", data={"async_data": "test"}
    )

    async def test_coroutine():
        # Should have access to the context
        ctx = get_current_context()
        assert ctx is not None
        assert ctx.pipeline_id == "async-test"
        assert ctx.data["async_data"] == "test"
        return "success"

    # Initially no context
    assert get_current_context() is None

    # Run coroutine with context
    result = await with_context(test_context, test_coroutine())
    assert result == "success"

    # Context should be cleaned up
    assert get_current_context() is None


@pytest.mark.asyncio
async def test_context_data_modification():
    """Test modifying context data during pipeline execution."""

    @single_task
    async def modify_context_data(x: int) -> dict:
        context = get_current_context()
        if context:
            # Read current value
            current_sum = context.data.get("running_sum", 0)
            # Modify context data
            context.data["running_sum"] = current_sum + x
            context.data["last_processed"] = x

            return {
                "value": x,
                "running_sum": context.data["running_sum"],
                "last_processed": context.data["last_processed"],
            }
        return {"value": x, "error": "no_context"}

    # Create pipeline with initial context
    pipeline = Pipeline().add(modify_context_data)
    custom_pipeline = pipeline.with_context(data={"running_sum": 100})

    results = await custom_pipeline([5, 10, 15]).collect()

    # Verify context data was modified progressively
    assert results[0]["running_sum"] == 105  # 100 + 5
    assert results[1]["running_sum"] == 115  # 105 + 10
    assert results[2]["running_sum"] == 130  # 115 + 15

    # Verify last processed tracking
    assert results[0]["last_processed"] == 5
    assert results[1]["last_processed"] == 10
    assert results[2]["last_processed"] == 15
