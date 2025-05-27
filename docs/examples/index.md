# Examples

Practical examples demonstrating Conveyor Streaming capabilities.

```{toctree}
:maxdepth: 2
:caption: Examples

basic-usage
```

## Overview

This section contains practical examples showing how to use Conveyor in real-world scenarios. More examples will be added as the library grows.

## Quick Examples

### Simple Pipeline
```python
from conveyor import single_task

@single_task
async def double(x: int) -> int:
    return x * 2

pipeline = double
results = await pipeline([1, 2, 3]).collect()
# [2, 4, 6]
```

### Batch Processing
```python
from conveyor import batch_task

@batch_task(max_size=3)
async def sum_batch(batch: list[int]) -> int:
    return sum(batch)

pipeline = sum_batch
async for result in pipeline([1, 2, 3, 4, 5]):
    print(result)  # 6, 9
```

## Available Examples

- **[Basic Usage](basic-usage.md)**: Simple examples to get started with Conveyor

More examples are being developed and will be added soon:
- Streaming patterns and advanced techniques
- Error handling and recovery strategies
- Performance optimization tips
- Real-world application scenarios