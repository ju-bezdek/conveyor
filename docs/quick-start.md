# Quick Start

Get up and running with Conveyor Streaming in 5 minutes.

## Installation

```bash
pip install conveyor-streaming
```

## Your First Pipeline

Let's build a simple data processing pipeline that demonstrates Conveyor's streaming capabilities:

```python
import asyncio
import random
from conveyor import single_task, batch_task

@single_task
async def fetch_user_data(user_id: int) -> dict:
    """Simulate fetching user data from an API."""
    # Simulate variable API response times
    await asyncio.sleep(random.uniform(0.1, 0.5))
    return {
        "id": user_id,
        "name": f"User {user_id}",
        "score": random.randint(1, 100)
    }

@batch_task(max_size=3)
async def calculate_batch_stats(users: list[dict]) -> dict:
    """Calculate statistics for a batch of users."""
    await asyncio.sleep(0.1)  # Simulate processing time
    total_score = sum(user["score"] for user in users)
    avg_score = total_score / len(users)
    
    return {
        "batch_size": len(users),
        "total_score": total_score,
        "average_score": round(avg_score, 2),
        "user_ids": [user["id"] for user in users]
    }

@single_task
async def format_result(stats: dict) -> str:
    """Format the statistics for display."""
    return (f"Batch of {stats['batch_size']} users "
            f"(IDs: {stats['user_ids']}) - "
            f"Average score: {stats['average_score']}")

async def main():
    # Create the pipeline
    pipeline = fetch_user_data | calculate_batch_stats | format_result
    
    # Process user IDs 1-10
    user_ids = list(range(1, 11))
    
    print("🚀 Starting pipeline - results will stream as they're ready!")
    print("-" * 60)
    
    # Stream results as they become available
    async for result in pipeline(user_ids):
        print(f"✅ {result}")
    
    print("-" * 60)
    print("✨ All done! Notice how results appeared as batches completed.")

if __name__ == "__main__":
    asyncio.run(main())
```

## What Just Happened?

1. **Streaming Processing**: Results appeared as soon as each batch of 3 users was processed, not after all 10 users were done
2. **Concurrent Execution**: All user fetches ran concurrently within each pipeline stage
3. **Memory Efficient**: Only small batches were kept in memory at once
4. **Order Preserved**: Results maintained the correct order despite concurrent processing

## Key Benefits Demonstrated

### Traditional Approach
```python
# Traditional: Wait for ALL, then process ALL
users = await fetch_all_users(user_ids)      # Wait 5 seconds
stats = await process_all_batches(users)     # Wait 2 more seconds
results = await format_all_results(stats)   # Wait 1 more second
# Total: 8 seconds before ANY results
```

### Conveyor Approach
```python
# Conveyor: Stream results as ready
async for result in pipeline(user_ids):
    print(result)  # First results in ~0.3 seconds!
# Continuous stream of results
```

## Common Patterns

### API Processing
```python
@single_task
async def call_api(url: str) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()

@single_task
async def process_response(data: dict) -> dict:
    return {"processed": True, "data": data}

api_pipeline = call_api | process_response
```

### Database Operations
```python
@batch_task(max_size=100)
async def bulk_insert(records: list[dict]) -> list[int]:
    async with database.transaction():
        return await database.bulk_insert(records)

@single_task
async def log_success(record_id: int) -> str:
    return f"Inserted record {record_id}"

db_pipeline = bulk_insert | log_success
```

### File Processing
```python
@single_task
async def parse_line(line: str) -> dict:
    return json.loads(line)

@batch_task(max_size=50)
async def validate_batch(records: list[dict]) -> list[dict]:
    return [r for r in records if is_valid(r)]

file_pipeline = parse_line | validate_batch
```

## Error Handling

Handle errors gracefully:

```python
@single_task(on_error="skip_item")
async def safe_process(data: dict) -> dict:
    if not data.get("id"):
        raise ValueError("Missing ID")
    return process_data(data)

@single_task(on_error="retry", max_retries=3)
async def reliable_api_call(url: str) -> dict:
    return await api_client.get(url)
```

## Advanced Features

### Side Inputs
```python
from conveyor import Context

@single_task
async def enrich_data(item: dict, context: Context) -> dict:
    lookup_table = context.get("lookup_table")
    item["enriched"] = lookup_table.get(item["id"])
    return item

# Use with context
context = Context({"lookup_table": my_lookup_data})
async for result in pipeline(data, context=context):
    process(result)
```

### Custom Streaming
```python
# Process items as they arrive
async def data_stream():
    async for item in external_data_source():
        yield item

async for result in pipeline(data_stream()):
    handle_result_immediately(result)
```

## Next Steps

**Ready to dive deeper?**

- Explore the [user guide](user-guide/index.md) for comprehensive documentation
- Check out [examples](examples/index.md) for real-world scenarios  
- Learn about [error handling](user-guide/error-handling.md) strategies
- Optimize performance with [performance tips](user-guide/performance.md)

## Performance Tips

1. **Tune Batch Sizes**: Start with small batches (10-50 items) and adjust based on your workload
2. **Use Concurrency Limits**: Prevent overwhelming external services
3. **Stream Large Datasets**: Use generators instead of loading everything into memory
4. **Monitor and Measure**: Add timing and metrics to optimize bottlenecks

Ready to build your own streaming pipelines? Start with the patterns above and adapt them to your specific use case!