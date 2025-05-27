# Tasks

Learn about Conveyor's task system - the building blocks of streaming pipelines.

## Overview

Tasks are the fundamental units of work in Conveyor pipelines. There are two main types:

- **Single Tasks** (`@single_task`) - Process one item at a time
- **Batch Tasks** (`@batch_task`) - Process multiple items together

## Single Tasks

Single tasks process items individually, enabling maximum concurrency and immediate result streaming.

### Basic Usage

```python
from conveyor import single_task

@single_task
async def process_item(item: str) -> str:
    # Process one item
    await asyncio.sleep(0.1)
    return item.upper()

# Use the task
results = await process_item(["hello", "world"]).collect()
# ["HELLO", "WORLD"]
```

### Configuration Options

```python
@single_task(
    on_error="skip_item",        # Error handling strategy
    retry_attempts=3,            # Retry failed items
    retry_delay=1.0,             # Initial retry delay
    retry_exponential_backoff=True,  # Use exponential backoff
    retry_max_delay=60.0,        # Maximum retry delay
    task_name="my_task"          # Custom task name for logging
)
async def robust_task(item: str) -> str:
    return await process_with_retries(item)
```

## Batch Tasks

Batch tasks group items together for efficient bulk processing.

### Basic Usage

```python
from conveyor import batch_task

@batch_task(max_size=5)
async def bulk_process(items: list[str]) -> list[str]:
    # Process batch of items
    await asyncio.sleep(0.2)
    return [item.upper() for item in items]

# Items are automatically batched
async for result in bulk_process(["a", "b", "c", "d", "e", "f"]):
    print(result)  # ["A", "B", "C", "D", "E"], ["F"]
```

### Batch Configuration

```python
@batch_task(
    min_size=2,                  # Minimum items per batch
    max_size=100,                # Maximum items per batch
    on_error="skip_batch",       # Error handling for batches
    retry_attempts=3,            # Retry failed batches
    retry_delay=1.0,             # Initial retry delay
    task_name="bulk_processor"   # Custom task name
)
async def optimized_batch(items: list[dict]) -> list[dict]:
    return await bulk_database_operation(items)
```

## Error Handling Strategies

### Skip Item/Batch
Continue processing other items when one fails:

```python
@single_task(on_error="skip_item")
async def safe_parse(text: str) -> dict:
    return json.loads(text)  # Invalid JSON is skipped

@batch_task(max_size=10, on_error="skip_batch")
async def safe_batch_process(items: list[str]) -> list[str]:
    return [item.upper() for item in items]  # Entire batch skipped on error
```

### Retry with Backoff
Automatically retry failed operations:

```python
@single_task(
    retry_attempts=3,
    retry_delay=1.0,
    retry_exponential_backoff=True,
    retry_max_delay=30.0,
    on_error="skip_item"  # Skip after all retries exhausted
)
async def api_call(url: str) -> dict:
    return await http_client.get(url)
```

### Fail Fast
Stop pipeline on first error (default behavior):

```python
@single_task  # on_error="fail" is the default
async def critical_task(item: str) -> str:
    if not item:
        raise ValueError("Empty item not allowed")
    return item
```

## Custom Error Handlers

For complex error handling scenarios:

```python
from conveyor import ErrorHandler

class CustomErrorHandler(ErrorHandler):
    async def handle_error(self, error: Exception, item, task_name: str, attempt: int) -> tuple[bool, any]:
        if isinstance(error, ValueError):
            print(f"Validation error in {task_name}: {error}")
            return True, None  # Continue, skip this item
        return False, None  # Re-raise other errors

@single_task(error_handler=CustomErrorHandler())
async def validated_task(item: str) -> str:
    if not item.strip():
        raise ValueError("Empty item")
    return item.upper()
```

## Performance Considerations

### Batch Size Optimization
Choose optimal batch sizes based on your use case:

```python
# Large batches for database operations
@batch_task(min_size=10, max_size=1000)
async def bulk_insert(records: list[dict]) -> list[int]:
    return await db.bulk_insert(records)

# Smaller batches for external APIs
@batch_task(max_size=10)
async def api_batch(items: list[str]) -> list[dict]:
    return await api.batch_process(items)
```

### Single vs Batch Tasks
- Use `@single_task` for:
  - I/O operations with high concurrency
  - Data transformations
  - When you need results immediately as they complete
  
- Use `@batch_task` for:
  - Database bulk operations
  - API calls with bulk endpoints
  - Operations that benefit from batching

## Advanced Features

### Side Inputs
Pass additional data to tasks:

```python
@single_task
async def enrich_data(item: dict, config: dict) -> dict:
    item["enriched"] = config["enrichment_value"]
    return item

# Use with side inputs
pipeline = enrich_data.with_inputs(config={"enrichment_value": "processed"})
results = await pipeline(data).collect()
```

### Custom Return Types
Tasks can return various types:

```python
@single_task
async def parse_and_validate(text: str) -> dict:
    data = json.loads(text)
    return {
        "parsed": data,
        "length": len(text),
        "valid": True
    }

@batch_task(max_size=5)
async def aggregate_results(items: list[dict]) -> dict:
    return {
        "count": len(items),
        "total_length": sum(item["length"] for item in items),
        "all_valid": all(item["valid"] for item in items)
    }
```

## Best Practices

### Keep Tasks Focused
Each task should have a single responsibility:

```python
# Good: Focused tasks
@single_task
async def validate_email(email: str) -> str:
    if "@" not in email:
        raise ValueError("Invalid email")
    return email

@single_task
async def send_notification(email: str) -> bool:
    return await email_service.send(email)

# Chain them: validate_email | send_notification
```

### Handle Errors Appropriately
- Use `on_error="skip_item"` for non-critical operations
- Use `on_error="fail"` (default) for critical pipeline steps
- Use retry logic for transient failures
- Use custom error handlers for complex scenarios

### Choose Appropriate Batch Sizes
- Consider memory usage vs. efficiency trade-offs
- Use `min_size` to ensure efficient batching
- Monitor and adjust based on performance metrics