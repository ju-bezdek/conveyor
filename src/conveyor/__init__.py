"""
Conveyor Streaming Library
A Python library for streamlining asynchronous streaming tasks and pipelines.
"""
import asyncio

from .stream import AsyncStream
from .tasks import BaseTask, SingleTask, BatchTask
from .pipeline import Pipeline
from .decorators import single_task, batch_task

# Example usage (optional, can be moved to an examples folder or documentation)
@single_task
async def multiply_by_two(x: int) -> int:
    print(f"Multiplying {x} by 2")
    await asyncio.sleep(0.01)
    return x * 2

@batch_task(min_size=2, max_size=3)
async def sum_batch(batch: list[int]) -> int:
    s = sum(batch)
    print(f"Summing batch: {batch} -> {s}")
    await asyncio.sleep(0.05)
    return s

@single_task
async def add_ten(x: int) -> int:
    print(f"Adding 10 to {x}")
    await asyncio.sleep(0.01)
    return x + 10

async def example_main():
    """Example main function to demonstrate pipeline usage."""
    pipeline = multiply_by_two | sum_batch | add_ten
    data_source = [1, 2, 3, 4, 5, 6, 7]

    print(f"Running pipeline with input: {data_source}")
    stream = pipeline(data_source)

    print("\nCollecting results:")
    results = await stream.collect()
    print(f"Collected: {results}")

    # Re-initialize stream for fresh consumption
    stream_consume = pipeline(data_source)
    print("\nStreaming results:")
    async for r in stream_consume:
        print(f"Streamed: {r}")

__all__ = [
    "AsyncStream",
    "BaseTask",
    "SingleTask",
    "BatchTask",
    "Pipeline",
    "single_task",
    "batch_task",
    "example_main" # Exposing example_main if it's intended to be runnable directly
]

if __name__ == "__main__":
    asyncio.run(example_main())