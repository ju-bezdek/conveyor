# Best Practices

This guide covers best practices for building efficient, maintainable, and robust streaming pipelines with Conveyor.

## Pipeline Design

### Keep Tasks Small and Focused
Each task should have a single responsibility:

```python
# Good: Focused tasks
@single_task
async def validate_email(email: str) -> str:
    if "@" not in email:
        raise ValueError("Invalid email")
    return email

@single_task
async def send_email(email: str) -> dict:
    # Send email logic
    return {"email": email, "sent": True}

# Better than one large task doing both
```

### Use Appropriate Task Types
Choose the right task type for your use case:

```python
# Use single_task for I/O operations
@single_task
async def fetch_user_data(user_id: int) -> dict:
    return await api_client.get_user(user_id)

# Use batch_task for operations that benefit from batching
@batch_task(max_size=50)
async def bulk_insert(records: list[dict]) -> list[int]:
    return await db.bulk_insert(records)
```

## Error Handling

### Use Defensive Programming
Always validate inputs and handle edge cases:

```python
@single_task
async def safe_divide(a: float, b: float) -> float:
    if b == 0:
        raise ValueError("Division by zero")
    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        raise TypeError("Arguments must be numbers")
    return a / b
```

### Choose Appropriate Error Strategies
Select error handling strategies based on your requirements:

```python
# Skip invalid items
@single_task(on_error="skip_item")
async def parse_json(text: str) -> dict:
    return json.loads(text)

# Retry with exponential backoff
@single_task(on_error="retry", max_retries=3, retry_delay=1.0, retry_backoff=2.0)
async def api_call(url: str) -> dict:
    return await http_client.get(url)
```

## Performance Optimization

### Optimize Batch Sizes
Tune batch sizes based on your specific workload:

```python
# For database operations, larger batches are often better
@batch_task(max_size=1000)
async def bulk_database_insert(records: list[dict]) -> list[int]:
    return await db.bulk_insert(records)

# For API calls with rate limits, smaller batches may be better
@batch_task(max_size=10)
async def api_batch_call(items: list[str]) -> list[dict]:
    return await api_client.batch_process(items)
```

### Use Concurrency Limits
Control resource usage with concurrency limits:

```python
@single_task(concurrency_limit=5)  # Max 5 concurrent requests
async def external_api_call(item: str) -> dict:
    return await external_api.process(item)
```

## Memory Management

### Process Large Datasets
Use generators for large datasets to maintain constant memory usage:

```python
async def process_large_file():
    async def read_lines():
        async with aiofiles.open("large_file.txt") as f:
            async for line in f:
                yield line.strip()
    
    # Memory usage stays constant regardless of file size
    async for result in pipeline(read_lines()):
        handle_result(result)
```

### Avoid Collecting Everything
Stream results instead of collecting all at once:

```python
# Good: Stream results
async for result in pipeline(data):
    await save_result(result)

# Avoid: Collecting all results in memory
# results = await pipeline(data).collect()  # Uses lots of memory
```

## Testing

### Test Individual Tasks
Write unit tests for each task:

```python
import pytest
from conveyor import single_task

@single_task
async def add_one(x: int) -> int:
    return x + 1

@pytest.mark.asyncio
async def test_add_one():
    result = await add_one(5)
    assert result == 6
```

### Test Pipeline Integration
Test complete pipelines with small datasets:

```python
@pytest.mark.asyncio
async def test_pipeline():
    pipeline = task1 | task2 | task3
    results = await pipeline([1, 2, 3]).collect()
    assert len(results) == 3
```

## Monitoring and Observability

### Add Logging
Include proper logging in your tasks:

```python
import logging

logger = logging.getLogger(__name__)

@single_task
async def process_item(item: dict) -> dict:
    logger.info(f"Processing item {item.get('id')}")
    try:
        result = await do_processing(item)
        logger.info(f"Successfully processed item {item.get('id')}")
        return result
    except Exception as e:
        logger.error(f"Failed to process item {item.get('id')}: {e}")
        raise
```

### Track Metrics
Monitor pipeline performance:

```python
import time
from conveyor import single_task

@single_task
async def timed_task(item: str) -> str:
    start_time = time.time()
    result = await process_item(item)
    duration = time.time() - start_time
    metrics.record_duration("task_duration", duration)
    return result
```

## Configuration Management

### Use Environment Variables
Keep configuration external:

```python
import os
from conveyor import Context

def create_context():
    return Context({
        "api_key": os.getenv("API_KEY"),
        "database_url": os.getenv("DATABASE_URL"),
        "max_retries": int(os.getenv("MAX_RETRIES", "3")),
    })
```

### Validate Configuration
Validate configuration at startup:

```python
def validate_config(context: Context):
    required_keys = ["api_key", "database_url"]
    for key in required_keys:
        if not context.get(key):
            raise ValueError(f"Missing required configuration: {key}")
```

## Documentation

### Document Task Behavior
Use clear docstrings:

```python
@single_task
async def normalize_email(email: str) -> str:
    """
    Normalize an email address to lowercase.
    
    Args:
        email: The email address to normalize
        
    Returns:
        The normalized email address
        
    Raises:
        ValueError: If the email format is invalid
    """
    if "@" not in email:
        raise ValueError("Invalid email format")
    return email.lower().strip()
```

### Document Pipeline Purpose
Document what your pipelines do:

```python
# User registration pipeline
# Validates email -> Creates user -> Sends welcome email -> Updates analytics
user_registration_pipeline = (
    validate_email | 
    create_user | 
    send_welcome_email | 
    update_analytics
)