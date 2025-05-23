import asyncio
from conveyor import single_task, Pipeline, AsyncStream

# Define a task that accepts a side input
@single_task
async def add_value_from_side_input(item: int, value_to_add: int) -> int:
    print(f"Item: {item}, Adding side value: {value_to_add}")
    await asyncio.sleep(0.01)
    return item + value_to_add

@single_task
async def multiply_by_two(x: int) -> int:
    print(f"Multiplying {x} by 2")
    await asyncio.sleep(0.01)
    return x * 2

# A coroutine that could be a side input
async def get_dynamic_value():
    await asyncio.sleep(0.1) # Simulate fetching data
    return 100

# An async generator for an AsyncStream side input
async def generate_stream_value():
    await asyncio.sleep(0.05)
    yield 50 # This first value will be used

async def main_with_side_inputs():
    results = {}
    data_source = [1, 2]

    # 1. Using a direct value as a side input
    pipeline_direct_value = (
        multiply_by_two |
        add_value_from_side_input.with_inputs(value_to_add=10) |
        multiply_by_two
    )
    results_direct = await pipeline_direct_value(data_source).collect()
    print(f"Results (direct value side input): {results_direct}")
    results["direct"] = results_direct

    # 2. Using a coroutine as a side input
    pipeline_coro_value = (
        multiply_by_two |
        add_value_from_side_input.with_inputs(value_to_add=get_dynamic_value()) |
        multiply_by_two
    )
    results_coro = await pipeline_coro_value(data_source).collect()
    print(f"Results (coroutine side input): {results_coro}")
    results["coroutine"] = results_coro

    # 3. Using an AsyncStream as a side input
    side_stream = AsyncStream(generate_stream_value())
    pipeline_stream_value = (
        multiply_by_two |
        add_value_from_side_input.with_inputs(value_to_add=side_stream) |
        multiply_by_two
    )
    results_stream = await pipeline_stream_value(data_source).collect()
    print(f"Results (AsyncStream side input): {results_stream}")
    results["stream"] = results_stream
    
    return results

if __name__ == "__main__":
    asyncio.run(main_with_side_inputs())
