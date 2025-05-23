import pytest
import asyncio
from conveyor.decorators import single_task, batch_task
from conveyor.tasks import SingleTask, BatchTask
from conveyor.stream import AsyncStream

def sample_sync_func_single(x):
    return x * 2

async def sample_async_func_single(x):
    return x * 3

def sample_sync_func_batch(batch):
    return [sum(batch)]

async def sample_async_func_batch(batch):
    return [sum(batch) * 2]

def test_single_task_decorator_sync():
    decorated_task = single_task(sample_sync_func_single)
    assert isinstance(decorated_task, SingleTask)
    assert decorated_task.func == sample_sync_func_single

def test_single_task_decorator_async():
    decorated_task = single_task(sample_async_func_single)
    assert isinstance(decorated_task, SingleTask)
    assert decorated_task.func == sample_async_func_single

def test_batch_task_decorator_sync_default_sizes():
    decorated_task = batch_task()(sample_sync_func_batch)
    assert isinstance(decorated_task, BatchTask)
    assert decorated_task.func == sample_sync_func_batch
    assert decorated_task.min_size == 1
    assert decorated_task.max_size == 1 # Defaults to min_size

def test_batch_task_decorator_async_custom_sizes():
    decorated_task = batch_task(min_size=2, max_size=5)(sample_async_func_batch)
    assert isinstance(decorated_task, BatchTask)
    assert decorated_task.func == sample_async_func_batch
    assert decorated_task.min_size == 2
    assert decorated_task.max_size == 5

def test_batch_task_decorator_min_size_only():
    decorated_task = batch_task(min_size=3)(sample_sync_func_batch)
    assert isinstance(decorated_task, BatchTask)
    assert decorated_task.func == sample_sync_func_batch
    assert decorated_task.min_size == 3
    assert decorated_task.max_size == 3 # Defaults to min_size


# Add bound mode test class
class TestProcessor:
    """Test class to verify bound mode functionality with decorated methods."""

    def __init__(self, multiplier: int = 1):
        self.multiplier = multiplier

    @single_task
    async def bound_single_task(self, x: int) -> int:
        """Bound method decorated as single task."""
        await asyncio.sleep(0.001)  # Simulate async work
        return x * self.multiplier

    @single_task
    def bound_single_task_sync(self, x: int) -> int:
        """Bound sync method decorated as single task."""
        return x + self.multiplier

    @batch_task(min_size=2, max_size=3)
    async def bound_batch_task(self, batch: list[int]) -> int:
        """Bound method decorated as batch task."""
        await asyncio.sleep(0.001)
        return sum(batch) * self.multiplier

    @batch_task(min_size=1, max_size=2)
    def bound_batch_task_sync(self, batch: list[int]) -> list[int]:
        """Bound sync method decorated as batch task."""
        return [x * self.multiplier for x in batch]


@pytest.mark.asyncio
async def test_bound_single_task_async():
    """Test bound async single task method."""
    processor = TestProcessor(multiplier=5)

    # Verify the decorator creates a SingleTask
    assert isinstance(processor.bound_single_task, SingleTask)

    # Test the bound task functionality
    input_data = [1, 2, 3]

    async def async_gen_from_list(data):
        for item in data:
            yield item

    stream = AsyncStream(async_gen_from_list(input_data))
    results = []

    async for item in await processor.bound_single_task.process(stream):
        results.append(item)

    assert results == [5, 10, 15]  # Each input multiplied by 5


@pytest.mark.asyncio
async def test_bound_single_task_sync():
    """Test bound sync single task method."""
    processor = TestProcessor(multiplier=10)

    # Verify the decorator creates a SingleTask
    assert isinstance(processor.bound_single_task_sync, SingleTask)

    # Test the bound task functionality
    input_data = [1, 2, 3]

    async def async_gen_from_list(data):
        for item in data:
            yield item

    stream = AsyncStream(async_gen_from_list(input_data))
    results = []

    async for item in await processor.bound_single_task_sync.process(stream):
        results.append(item)

    assert results == [11, 12, 13]  # Each input plus 10


@pytest.mark.asyncio
async def test_bound_batch_task_async():
    """Test bound async batch task method."""
    processor = TestProcessor(multiplier=3)

    # Verify the decorator creates a BatchTask
    assert isinstance(processor.bound_batch_task, BatchTask)
    assert processor.bound_batch_task.min_size == 2
    assert processor.bound_batch_task.max_size == 3

    # Test the bound task functionality
    input_data = [1, 2, 3, 4, 5]

    async def async_gen_from_list(data):
        for item in data:
            yield item

    stream = AsyncStream(async_gen_from_list(input_data))
    results = []

    async for item in await processor.bound_batch_task.process(stream):
        results.append(item)

    # Batch 1: [1,2,3] -> sum=6 -> 6*3=18
    # Batch 2: [4,5] -> sum=9 -> 9*3=27 (remainder >= min_size)
    assert results == [18, 27]


@pytest.mark.asyncio
async def test_bound_batch_task_sync():
    """Test bound sync batch task method."""
    processor = TestProcessor(multiplier=2)

    # Verify the decorator creates a BatchTask
    assert isinstance(processor.bound_batch_task_sync, BatchTask)
    assert processor.bound_batch_task_sync.min_size == 1
    assert processor.bound_batch_task_sync.max_size == 2

    # Test the bound task functionality
    input_data = [1, 2, 3, 4]

    async def async_gen_from_list(data):
        for item in data:
            yield item

    stream = AsyncStream(async_gen_from_list(input_data))
    results = []

    async for item in await processor.bound_batch_task_sync.process(stream):
        results.append(item)

    # Batch 1: [1,2] -> [2,4]
    # Batch 2: [3,4] -> [6,8]
    # Since the method returns a list, each element is yielded separately
    assert results == [2, 4, 6, 8]


@pytest.mark.asyncio
async def test_bound_tasks_in_pipeline():
    """Test bound tasks can be used in pipelines."""
    processor1 = TestProcessor(multiplier=2)
    processor2 = TestProcessor(multiplier=3)

    # Create a pipeline using bound tasks
    pipeline = processor1.bound_single_task_sync | processor2.bound_single_task_sync

    input_data = [1, 2, 3]
    stream = pipeline(input_data)
    results = await stream.collect()

    # First: x + 2, Second: result + 3
    # 1 -> 3 -> 6, 2 -> 4 -> 7, 3 -> 5 -> 8
    assert results == [6, 7, 8]


def test_bound_task_preserves_instance_state():
    """Test that bound tasks preserve access to instance state."""
    processor1 = TestProcessor(multiplier=5)
    processor2 = TestProcessor(multiplier=10)

    # Verify different instances have different behavior
    assert isinstance(processor1.bound_single_task_sync, SingleTask)
    assert isinstance(processor2.bound_single_task_sync, SingleTask)

    # The tasks should be different objects but same type
    assert processor1.bound_single_task_sync is not processor2.bound_single_task_sync
    assert type(processor1.bound_single_task_sync) == type(
        processor2.bound_single_task_sync
    )
