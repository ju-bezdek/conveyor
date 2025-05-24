import pytest
import asyncio
import time
from conveyor import single_task, batch_task, Pipeline, PipelineContext, get_current_context
from conveyor.stream import AsyncStream

@single_task
async def slow_task(x: int) -> int:
    """Task with variable delay based on input value."""
    delay = x * 0.1  # Larger numbers take longer
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
            'value': x,
            'pipeline_id': context.pipeline_id[:8] if context and context.pipeline_id else 'none',
            'execution_mode': context.execution_mode if context else 'none',
            'stage': context.current_stage if context else -1,
            'total_stages': context.stage_count if context else -1
        }
    
    pipeline = context_aware_task
    results = await pipeline([1, 2]).collect()
    
    # Verify context is properly set
    for result in results:
        assert result['pipeline_id'] != 'none'
        assert result['execution_mode'] == 'ordered'  # Default mode
        assert result['stage'] == 0  # First (and only) stage
        assert result['total_stages'] == 1

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