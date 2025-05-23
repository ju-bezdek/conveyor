\
import pytest
from conveyor.decorators import single_task, batch_task
from conveyor.tasks import SingleTask, BatchTask

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
