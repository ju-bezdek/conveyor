# Basic Usage

This section demonstrates the fundamental concepts of Conveyor Streaming with simple, practical examples.

## Single Task Example

The most basic building block is a single task that processes one item at a time:

```python
import asyncio
from conveyor import single_task

@single_task
async def multiply_by_two(x: int) -> int:
    """Multiply a number by 2."""
    await asyncio.sleep(0.01)  # Simulate async work
    return x * 2

async def main():
    # Process a list of numbers
    data = [1, 2, 3, 4, 5]
    
    # Stream results as they become available
    async for result in multiply_by_two(data):
        print(f"Result: {result}")
    
    # Or collect all results at once
    results = await multiply_by_two(data).collect()
    print(f"All results: {results}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Batch Task Example

Batch tasks process multiple items together, which can be more efficient for certain operations:

```python
from conveyor import batch_task

@batch_task(max_size=3)
async def sum_batch(numbers: list[int]) -> int:
    """Sum a batch of numbers."""
    await asyncio.sleep(0.05)  # Simulate batch processing
    return sum(numbers)

async def main():
    data = [1, 2, 3, 4, 5, 6, 7]
    
    # Process in batches
    async for batch_sum in sum_batch(data):
        print(f"Batch sum: {batch_sum}")
    # Output: 6 (1+2+3), 15 (4+5+6), 7 (remaining item)

if __name__ == "__main__":
    asyncio.run(main())
```

## Pipeline Composition

Chain tasks together using the `|` operator:

```python
@single_task
async def add_one(x: int) -> int:
    return x + 1

@single_task
async def multiply_by_three(x: int) -> int:
    return x * 3

async def main():
    # Create a pipeline
    pipeline = add_one | multiply_by_three
    
    # Process data through the pipeline
    data = [1, 2, 3]
    results = await pipeline(data).collect()
    print(results)  # [6, 9, 12] (each number + 1, then * 3)

if __name__ == "__main__":
    asyncio.run(main())
```

## Mixed Pipeline Example

Combine single tasks and batch tasks in one pipeline:

```python
@single_task
async def validate_positive(x: int) -> int:
    """Ensure number is positive."""
    if x <= 0:
        raise ValueError(f"Number must be positive, got {x}")
    return x

@batch_task(max_size=2)
async def batch_square(numbers: list[int]) -> list[int]:
    """Square each number in the batch."""
    return [x ** 2 for x in numbers]

@single_task
async def format_result(x: int) -> str:
    """Format the result as a string."""
    return f"Result: {x}"

async def main():
    # Create mixed pipeline
    pipeline = validate_positive | batch_square | format_result
    
    data = [1, 2, 3, 4, 5]
    
    async for result in pipeline(data):
        print(result)

if __name__ == "__main__":
    asyncio.run(main())
```

## Error Handling

Handle errors gracefully in your pipelines:

```python
@single_task(on_error="skip_item")
async def safe_divide(x: float) -> float:
    """Divide by a random number, might fail."""
    import random
    divisor = random.choice([0, 1, 2])  # Sometimes 0!
    if divisor == 0:
        raise ZeroDivisionError("Cannot divide by zero")
    return x / divisor

async def main():
    data = [10.0, 20.0, 30.0, 40.0, 50.0]
    
    # Some items will be skipped due to division by zero
    results = await safe_divide(data).collect()
    print(f"Successful results: {results}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Using Context

Pass shared data through your pipeline:

```python
from conveyor import Context

@single_task
async def add_prefix(text: str, context: Context) -> str:
    """Add a prefix from context to the text."""
    prefix = context.get("prefix", "")
    return f"{prefix}{text}"

async def main():
    # Create context with shared data
    context = Context({"prefix": "Hello, "})
    
    data = ["Alice", "Bob", "Charlie"]
    
    async for result in add_prefix(data, context=context):
        print(result)  # "Hello, Alice", "Hello, Bob", etc.

if __name__ == "__main__":
    asyncio.run(main())
```

## Generator Input

Process data from generators for memory efficiency:

```python
async def number_generator():
    """Generate numbers asynchronously."""
    for i in range(10):
        await asyncio.sleep(0.1)  # Simulate data arriving over time
        yield i

@single_task
async def process_number(x: int) -> str:
    return f"Processed: {x}"

async def main():
    # Process generator input
    gen = number_generator()
    
    async for result in process_number(gen):
        print(result)  # Results appear as data is generated

if __name__ == "__main__":
    asyncio.run(main())
```