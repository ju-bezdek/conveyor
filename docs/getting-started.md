# Getting Started

Welcome to Conveyor Streaming! This guide will help you understand the core concepts and get your first pipeline running.

## Core Concepts

### Tasks

Tasks are the building blocks of Conveyor pipelines. There are two main types:

- **Single Tasks** (`@single_task`): Process one item at a time
- **Batch Tasks** (`@batch_task`): Process multiple items together in batches

### Pipelines

Pipelines chain tasks together using the `|` operator, creating powerful data processing workflows that can stream results as they become available.

### Streaming

Conveyor's key innovation is its ability to stream intermediate results between pipeline stages, enabling:
- Early result delivery
- Better resource utilization
- Preserved order when needed
- Flexible consumption patterns

## Your First Pipeline

Let's build a simple pipeline step by step:

```python
import asyncio
from conveyor import single_task, batch_task

# Step 1: Define tasks
@single_task
async def multiply_by_two(x: int) -> int:
    print(f"Multiplying {x} by 2")
    await asyncio.sleep(0.01)  # Simulate async work
    return x * 2

@batch_task(max_size=3)
async def sum_batch(batch: list[int]) -> int:
    print(f"Summing batch: {batch}")
    await asyncio.sleep(0.05)  # Simulate batch processing
    return sum(batch)

@single_task
async def add_ten(x: int) -> int:
    print(f"Adding 10 to {x}")
    await asyncio.sleep(0.01)
    return x + 10

# Step 2: Create pipeline
pipeline = multiply_by_two | sum_batch | add_ten

# Step 3: Use the pipeline
async def main():
    data = [1, 2, 3, 4, 5, 6, 7]
    
    # Option 1: Stream results as they come
    print("Streaming results:")
    async for result in pipeline(data):
        print(f"Got result: {result}")
    
    # Option 2: Collect all results
    print("\nCollecting all results:")
    results = await pipeline(data).collect()
    print(f"All results: {results}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Understanding the Flow

The pipeline above processes data as follows:

```{mermaid}
graph TD
    A["Input: [1,2,3,4,5,6,7]"] --> B[multiply_by_two]
    B --> C["[2,4,6,8,10,12,14]"]
    C --> D[sum_batch max_size=3]
    D --> E["Batch 1: [2,4,6] → 12"]
    D --> F["Batch 2: [8,10,12] → 30"] 
    D --> G["Batch 3: [14] → 14"]
    E --> H[add_ten]
    F --> I[add_ten]
    G --> J[add_ten]
    H --> K["Result: 22"]
    I --> L["Result: 40"]
    J --> M["Result: 24"]
```

## Key Benefits Demonstrated

1. **Concurrency**: All items are processed concurrently within each stage
2. **Streaming**: Results are available as soon as each batch completes
3. **Efficiency**: No waiting for all items to complete before starting the next stage
4. **Simplicity**: Clean, readable code with decorator-based task definition

## Next Steps

```{admonition} What's Next?
:class: tip

- Read the {doc}`quick-start` for more detailed examples
- Explore {doc}`user-guide/tasks` to learn about task types and options
- Check out {doc}`user-guide/streaming` for advanced streaming patterns
- See {doc}`examples/index` for real-world use cases
```

## Common Patterns

### Processing Lists vs Generators

```python
# Works with lists
data = [1, 2, 3, 4, 5]
results = await pipeline(data).collect()

# Works with async generators
async def data_generator():
    for i in range(1, 6):
        yield i
        await asyncio.sleep(0.1)

async for result in pipeline(data_generator()):
    print(result)
```

### Error Handling

```python
@single_task(on_error="skip_item")
async def safe_task(x: int) -> int:
    if x < 0:
        raise ValueError("Negative numbers not allowed")
    return x * 2
```

### Batch Size Control

```python
@batch_task(max_size=5, min_size=2)
async def flexible_batch(batch: list[int]) -> int:
    # Processes batches of 2-5 items
    # Remainder items only processed if >= min_size
    return sum(batch)
```