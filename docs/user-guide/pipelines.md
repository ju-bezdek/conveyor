# Pipelines

Learn how to compose and execute Conveyor pipelines for complex data processing workflows.

## Overview

Pipelines in Conveyor chain tasks together to create powerful data processing workflows. They enable you to:

- Compose complex processing logic from simple tasks
- Stream intermediate results between stages
- Handle errors at the pipeline level
- Optimize performance across the entire workflow

## Basic Pipeline Composition

### Using the Pipe Operator

The simplest way to create pipelines is using the `|` operator:

```python
from conveyor import single_task, batch_task

@single_task
async def clean_data(item: str) -> str:
    return item.strip().lower()

@single_task
async def validate_data(item: str) -> str:
    if not item:
        raise ValueError("Empty item")
    return item

@batch_task(max_size=5)
async def process_batch(items: list[str]) -> list[str]:
    return [f"processed: {item}" for item in items]

# Create pipeline
pipeline = clean_data | validate_data | process_batch
```

### Manual Pipeline Creation

For more control, you can create pipelines manually:

```python
from conveyor.pipeline import Pipeline

pipeline = Pipeline([clean_data, validate_data, process_batch])
```

## Pipeline Execution

### Streaming Results

Get results as they become available:

```python
data = ["  Hello  ", "  WORLD  ", "", "  Python  "]

async for result in pipeline(data):
    print(result)  # Results stream as batches complete
```

### Collecting All Results

Wait for all processing to complete:

```python
results = await pipeline(data).collect()
print(results)  # All results at once
```

### Using Generators

Process data from generators for memory efficiency:

```python
async def data_generator():
    for i in range(1000):
        yield f"item_{i}"

async for result in pipeline(data_generator()):
    handle_result(result)
```

## Advanced Pipeline Patterns

### Conditional Processing

Create pipelines that adapt based on data:

```python
@single_task
async def route_data(item: dict) -> dict:
    item["route"] = "premium" if item.get("priority") else "standard"
    return item

@single_task
async def process_premium(item: dict) -> dict:
    if item.get("route") == "premium":
        # Special processing for premium items
        return await premium_processing(item)
    return item

@single_task
async def process_standard(item: dict) -> dict:
    if item.get("route") == "standard":
        # Standard processing
        return await standard_processing(item)
    return item

pipeline = route_data | process_premium | process_standard
```

### Fan-out Pattern

Process items through multiple parallel paths:

```python
@single_task
async def duplicate_item(item: str) -> list[str]:
    return [item, item]  # Fan out to multiple copies

@single_task
async def process_copy(item: str) -> str:
    return f"processed: {item}"

# This will process each item twice
pipeline = duplicate_item | process_copy
```

### Aggregation Pattern

Combine results from multiple items:

```python
@batch_task(max_size=10)
async def aggregate_results(items: list[dict]) -> dict:
    return {
        "count": len(items),
        "total_value": sum(item.get("value", 0) for item in items),
        "items": items
    }

@single_task
async def format_summary(summary: dict) -> str:
    return f"Processed {summary['count']} items, total: {summary['total_value']}"

pipeline = process_item | aggregate_results | format_summary
```

## Pipeline Context

### Sharing Data Across Tasks

Use context to share data between pipeline stages:

```python
from conveyor import Context

@single_task
async def enrich_with_metadata(item: dict, context: Context) -> dict:
    metadata = context.get("metadata", {})
    item.update(metadata)
    return item

# Create context with shared data
context = Context({
    "metadata": {"version": "1.0", "environment": "prod"}
})

# Execute with context
async for result in pipeline(data, context=context):
    print(result)
```

### Dynamic Context Updates

Tasks can modify context for downstream tasks:

```python
@single_task
async def track_processing(item: dict, context: Context) -> dict:
    # Update context with processing stats
    processed_count = context.get("processed_count", 0) + 1
    context.set("processed_count", processed_count)
    return item

@single_task
async def add_sequence_number(item: dict, context: Context) -> dict:
    seq_num = context.get("processed_count", 0)
    item["sequence"] = seq_num
    return item

pipeline = track_processing | add_sequence_number
```

## Error Handling in Pipelines

### Pipeline-Level Error Handling

Handle errors across the entire pipeline:

```python
@single_task(on_error="skip_item")
async def safe_parse(text: str) -> dict:
    return json.loads(text)

@single_task(on_error="retry", max_retries=3)
async def reliable_api_call(data: dict) -> dict:
    return await api_client.post(data)

# Errors are handled at each stage independently
pipeline = safe_parse | reliable_api_call
```

### Custom Error Handling

Implement custom error handling logic:

```python
async def process_with_error_handling(data):
    try:
        async for result in pipeline(data):
            yield result
    except Exception as e:
        # Custom error handling
        logger.error(f"Pipeline failed: {e}")
        # Maybe retry, or process remaining items
```

## Performance Optimization

### Batch Size Tuning

Optimize batch sizes for your workload:

```python
# Small batches for low-latency processing
@batch_task(max_size=5)
async def low_latency_batch(items: list[str]) -> list[str]:
    return await quick_processing(items)

# Large batches for high-throughput processing
@batch_task(max_size=1000)
async def high_throughput_batch(items: list[dict]) -> list[dict]:
    return await bulk_database_operation(items)
```

### Concurrency Control

Control resource usage across the pipeline:

```python
@single_task(concurrency_limit=10)
async def api_task(item: dict) -> dict:
    return await external_api.process(item)

@single_task(concurrency_limit=5)
async def database_task(item: dict) -> dict:
    return await database.save(item)

# Each stage respects its own concurrency limits
pipeline = api_task | database_task
```

## Pipeline Composition Patterns

### Reusable Sub-pipelines

Create reusable pipeline components:

```python
# Data cleaning pipeline
data_cleaning = clean_text | validate_format | normalize_encoding

# Data enrichment pipeline
data_enrichment = lookup_metadata | add_timestamps | calculate_metrics

# Combine into complete pipeline
complete_pipeline = data_cleaning | data_enrichment | save_results
```

### Pipeline Factories

Create pipelines dynamically:

```python
def create_processing_pipeline(batch_size: int, api_endpoint: str):
    @batch_task(max_size=batch_size)
    async def custom_batch_processor(items: list[dict]) -> list[dict]:
        return await process_batch_with_api(items, api_endpoint)
    
    return validate_input | custom_batch_processor | format_output

# Create different pipelines for different environments
dev_pipeline = create_processing_pipeline(10, "https://dev-api.com")
prod_pipeline = create_processing_pipeline(100, "https://prod-api.com")
```

## Testing Pipelines

### Unit Testing Individual Stages

Test each task independently:

```python
import pytest

@pytest.mark.asyncio
async def test_clean_data():
    result = await clean_data("  Hello World  ")
    assert result == "hello world"

@pytest.mark.asyncio
async def test_validate_data():
    with pytest.raises(ValueError):
        await validate_data("")
```

### Integration Testing

Test complete pipelines:

```python
@pytest.mark.asyncio
async def test_complete_pipeline():
    test_data = ["  Hello  ", "  World  "]
    results = await pipeline(test_data).collect()
    
    assert len(results) == 1  # One batch result
    assert all("processed:" in item for item in results[0])
```

## Best Practices

### Keep Pipelines Simple
- Prefer many simple tasks over few complex ones
- Each task should have a single responsibility
- Use descriptive names for tasks and pipelines

### Handle Errors Gracefully
- Choose appropriate error handling strategies for each task
- Consider the impact of errors on downstream processing
- Implement monitoring and alerting for production pipelines

### Optimize for Your Use Case
- Tune batch sizes based on your data and operations
- Use concurrency limits to control resource usage
- Monitor performance and adjust as needed