# Tasks API

## Task Classes

```{eval-rst}
.. automodule:: conveyor.tasks
   :members:
   :undoc-members:
   :show-inheritance:
```

## Task Decorators

```{eval-rst}
.. automodule:: conveyor.decorators
   :members:
   :undoc-members:
   :show-inheritance:
```

## Usage Examples

### Basic Single Task Definition

```python
from conveyor import single_task

@single_task
async def process_data(item):
    """Process a single data item."""
    return item.upper()

# Or with error handling options
@single_task(
    on_error="skip_item",
    retry_attempts=3,
    retry_delay=1.0
)
async def robust_process(item):
    """Process with retry logic."""
    return await some_api_call(item)
```

### Batch Processing

```python
from conveyor import batch_task

@batch_task(max_size=100)
async def process_batch(items):
    """Process items in batches."""
    return [item.upper() for item in items]

# With minimum batch size
@batch_task(min_size=5, max_size=20)
async def efficient_batch(items):
    """Process with minimum batch requirements."""
    return await bulk_operation(items)
```