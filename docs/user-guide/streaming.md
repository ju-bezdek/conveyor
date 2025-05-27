# Streaming

Streaming is Conveyor's key innovation - the ability to process and yield results as they become available, rather than waiting for all processing to complete. This enables better responsiveness, resource utilization, and user experience.

## Streaming Concepts

### Traditional vs Streaming Processing

**Traditional Batch Processing:**
```python
# Traditional approach - wait for ALL results
async def traditional_processing(items):
    results = []
    for item in items:
        result = await process_item(item)
        results.append(result)
    return results  # All results available at once, at the end

all_results = await traditional_processing(large_dataset)
print(f"Got {len(all_results)} results")  # Long wait, then everything at once
```

**Conveyor Streaming:**
```python
# Streaming approach - results as they're ready
pipeline = process_item_task | transform_task | validate_task

async for result in pipeline(large_dataset):
    print(f"Got result: {result}")  # Results appear immediately as ready
    # Can process, display, or save each result immediately
```

### Performance Benefits

```{admonition} Streaming Performance Timeline
:class: tip

**Example with 8 tasks taking [0.1s, 0.2s, 0.3s, 0.4s, 0.5s, 2.0s, 0.7s, 0.8s]:**

**Traditional approach:**
- Time: 2.0s → ALL results yielded at once

**Conveyor streaming approach:**
- Time: 0.1s → yield result 1 ⚡ 
- Time: 0.2s → yield result 2 ⚡
- Time: 0.3s → yield result 3 ⚡
- Time: 0.4s → yield result 4 ⚡
- Time: 0.5s → yield result 5 ⚡
- Time: 2.0s → yield results 6,7,8 (7,8 buffered, waiting for 6)

**Result: First 5 results available 75% faster!**
```

## Execution Modes

### Ordered Streaming (Default)

Preserves input order while streaming results as early as possible:

```python
@single_task
async def variable_delay_task(item: tuple[str, float]) -> str:
    name, delay = item
    await asyncio.sleep(delay)
    return f"Processed {name}"

async def ordered_streaming_demo():
    items = [
        ("fast", 0.1),
        ("medium", 0.5), 
        ("slow", 2.0),
        ("quick", 0.2)
    ]
    
    pipeline = variable_delay_task
    
    print("Ordered streaming:")
    async for result in pipeline(items):
        print(f"  {asyncio.get_event_loop().time():.1f}s: {result}")
    
    # Output order matches input order:
    # 0.1s: Processed fast
    # 0.5s: Processed medium  
    # 2.0s: Processed slow     <- blocks subsequent results
    # 2.0s: Processed quick    <- immediately after slow completes
```

### As-Completed Streaming

Yields results as they complete, regardless of input order:

```python
async def as_completed_demo():
    items = [
        ("fast", 0.1),
        ("medium", 0.5),
        ("slow", 2.0), 
        ("quick", 0.2)
    ]
    
    pipeline = variable_delay_task
    
    print("As-completed streaming:")
    async for result in pipeline.as_completed(items):
        print(f"  {asyncio.get_event_loop().time():.1f}s: {result}")
    
    # Output in completion order:
    # 0.1s: Processed fast    <- fastest first
    # 0.2s: Processed quick   <- second fastest
    # 0.5s: Processed medium  <- third fastest
    # 2.0s: Processed slow    <- slowest last
```

### Pipeline-Level Execution Mode

Set execution mode for entire pipeline:

```python
# Create pipeline with specific execution mode
fast_pipeline = pipeline.with_execution_mode("as_completed")

async for result in fast_pipeline(data):
    print(f"Result in completion order: {result}")

# Or use context
from conveyor import PipelineContext

context = PipelineContext(execution_mode="as_completed")
contextualized_pipeline = pipeline.with_context(**context.__dict__)
```

## Streaming Patterns

### Real-time Processing

Stream results for immediate processing:

```python
@single_task
async def fetch_stock_price(symbol: str) -> dict:
    # Simulate API call with variable latency
    await asyncio.sleep(random.uniform(0.1, 1.0))
    return {
        "symbol": symbol,
        "price": random.uniform(50, 200),
        "timestamp": time.time()
    }

@single_task
async def check_alerts(price_data: dict) -> dict | None:
    """Filter for prices that trigger alerts"""
    if price_data["price"] > 150:
        return {**price_data, "alert": "HIGH_PRICE"}
    return None

async def real_time_monitoring():
    symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
    pipeline = fetch_stock_price | check_alerts
    
    print("Monitoring stock prices (streaming alerts)...")
    async for alert in pipeline(symbols):
        # Process alerts immediately as they're detected
        print(f"🚨 ALERT: {alert['symbol']} at ${alert['price']:.2f}")
        await send_notification(alert)
```

### Progress Tracking

Show progress as items complete:

```python
@single_task
async def process_document(doc_path: str) -> dict:
    """Process a document (variable processing time)"""
    size = os.path.getsize(doc_path)
    processing_time = size / 1000000  # Simulate size-based processing
    
    await asyncio.sleep(processing_time)
    return {
        "path": doc_path,
        "size": size,
        "processed_at": time.time()
    }

async def batch_document_processing():
    documents = get_document_list()  # Large list of documents
    pipeline = process_document
    
    total_docs = len(documents)
    processed_count = 0
    
    print(f"Processing {total_docs} documents...")
    
    async for result in pipeline(documents):
        processed_count += 1
        progress = (processed_count / total_docs) * 100
        
        print(f"[{progress:5.1f}%] Processed: {result['path']}")
        
        # Update UI, save checkpoint, etc.
        await update_progress_bar(progress)
```

### Early Results for User Experience

Show results immediately for better UX:

```python
@single_task
async def search_database(query: str) -> list[dict]:
    """Search returns results of varying relevance"""
    await asyncio.sleep(random.uniform(0.1, 2.0))  # Variable search time
    
    # Return multiple results
    return [
        {"query": query, "result": f"Result {i}", "relevance": random.random()}
        for i in range(random.randint(1, 5))
    ]

@single_task
async def rank_result(result: dict) -> dict:
    """Add ranking information"""
    await asyncio.sleep(0.1)  # Ranking processing
    return {**result, "ranked": True, "score": result["relevance"] * 100}

async def search_interface():
    queries = ["python", "async", "streaming", "pipeline", "batch"]
    pipeline = search_database | rank_result
    
    print("Search results (streaming):")
    
    async for result in pipeline.as_completed(queries):
        # Show results immediately as they're ready
        print(f"  {result['query']}: {result['result']} (score: {result['score']:.1f})")
        # Update UI immediately - no waiting for all searches
```

## Collection vs Streaming

### When to Stream

**Use streaming when:**
- Results can be processed independently
- User experience benefits from immediate feedback
- Memory usage needs to be controlled
- Early results provide value

```python
# Good for streaming - independent results
async for user in pipeline(user_ids):
    await send_email(user)  # Each result processed independently

# Good for streaming - progress tracking
async for processed_file in pipeline(file_list):
    update_progress_display(processed_file)

# Good for streaming - real-time updates
async for stock_price in pipeline(symbols):
    update_dashboard(stock_price)
```

### When to Collect

**Use collection when:**
- Need all results for final computation
- Results must be processed together
- Order matters for final output

```python
# Good for collection - need all results
all_prices = await pipeline(symbols).collect()
average_price = sum(p["price"] for p in all_prices) / len(all_prices)

# Good for collection - batch operations
all_users = await pipeline(user_data).collect()
await database.bulk_insert(all_users)

# Good for collection - sorting final results
results = await pipeline(queries).collect()
sorted_results = sorted(results, key=lambda x: x["score"], reverse=True)
```

## Advanced Streaming Patterns

### Buffered Streaming

Control memory usage with buffering:

```python
@batch_task(max_size=10, min_size=1)
async def buffered_processor(items: list[dict]) -> list[dict]:
    """Process in controlled batches"""
    print(f"Processing batch of {len(items)} items")
    await asyncio.sleep(0.5)  # Batch processing time
    return [{"processed": True, **item} for item in items]

async def controlled_streaming():
    large_dataset = range(1000)  # Large dataset
    
    pipeline = create_item | buffered_processor
    
    # Results stream in batches, controlling memory usage
    batch_count = 0
    async for result in pipeline(large_dataset):
        if batch_count % 50 == 0:  # Log every 50 items
            print(f"Processed item {result}")
        batch_count += 1
```

### Multi-source Streaming

Combine multiple input sources:

```python
@single_task
async def fetch_from_api(endpoint: str) -> list[dict]:
    """Fetch data from API endpoint"""
    async with aiohttp.ClientSession() as session:
        async with session.get(endpoint) as response:
            return await response.json()

async def multi_source_processing():
    # Multiple data sources
    endpoints = [
        "https://api1.example.com/data",
        "https://api2.example.com/data", 
        "https://api3.example.com/data"
    ]
    
    pipeline = fetch_from_api
    
    # Stream results from all sources as they complete
    all_data = []
    async for data_batch in pipeline.as_completed(endpoints):
        print(f"Received {len(data_batch)} items from source")
        all_data.extend(data_batch)
        
        # Process immediately, don't wait for all sources
        await process_batch_immediately(data_batch)
```

### Conditional Streaming

Stream different outputs based on conditions:

```python
@single_task
async def classify_and_route(item: dict) -> dict:
    """Classify item and add routing info"""
    # Classification logic
    category = await classify_item(item)
    return {**item, "category": category, "route": get_route(category)}

@single_task 
async def route_processor(item: dict) -> dict:
    """Process based on routing"""
    route = item["route"]
    
    if route == "fast_track":
        return await fast_process(item)
    elif route == "bulk_track": 
        return await bulk_process(item)
    else:
        return await default_process(item)

async def conditional_streaming():
    pipeline = classify_and_route | route_processor
    
    # Different items may take different processing paths
    async for result in pipeline(mixed_data):
        category = result.get("category")
        
        if category == "urgent":
            await handle_urgent(result)
        else:
            await handle_normal(result)
```

## Performance Optimization

### Execution Mode Selection

```python
# For order-sensitive processing
ordered_pipeline = pipeline  # Default is ordered

# For maximum performance when order doesn't matter  
fast_pipeline = pipeline.with_execution_mode("as_completed")

# Performance comparison
start_time = time.time()

# Ordered processing
async for result in ordered_pipeline(data):
    pass
ordered_time = time.time() - start_time

start_time = time.time()

# As-completed processing  
async for result in fast_pipeline(data):
    pass
as_completed_time = time.time() - start_time

print(f"Ordered: {ordered_time:.2f}s, As-completed: {as_completed_time:.2f}s")
```

### Batch Size Tuning for Streaming

```python
# Small batches: Lower latency, more streaming opportunities
@batch_task(max_size=5, min_size=1)
async def low_latency_batch(items):
    return process_small_batch(items)

# Large batches: Higher throughput, less frequent streaming
@batch_task(max_size=100, min_size=10) 
async def high_throughput_batch(items):
    return process_large_batch(items)

# Choose based on requirements:
# - Real-time systems: small batches
# - Bulk processing: large batches
```

### Memory-Conscious Streaming

```python
async def memory_efficient_processing():
    """Process large datasets without loading all into memory"""
    
    async def data_generator():
        """Generate data on-demand"""
        for i in range(1_000_000):  # Very large dataset
            yield {"id": i, "data": f"item_{i}"}
            if i % 10000 == 0:
                # Yield control periodically
                await asyncio.sleep(0)
    
    pipeline = validate_item | transform_item | save_item
    
    # Process streaming data without memory buildup
    processed_count = 0
    async for result in pipeline(data_generator()):
        processed_count += 1
        
        if processed_count % 1000 == 0:
            print(f"Processed {processed_count} items")
            # Memory usage stays constant due to streaming
```

## Error Handling in Streaming

### Graceful Degradation

```python
@single_task(on_error="skip_item")
async def fault_tolerant_task(item: dict) -> dict:
    """Continue streaming even if some items fail"""
    if item.get("corrupted"):
        raise ValueError("Corrupted data")
    
    return {"processed": True, **item}

async def resilient_streaming():
    mixed_data = [
        {"id": 1, "valid": True},
        {"id": 2, "corrupted": True},  # Will be skipped
        {"id": 3, "valid": True},
    ]
    
    pipeline = fault_tolerant_task
    
    # Streaming continues despite errors
    async for result in pipeline(mixed_data):
        print(f"Successfully processed: {result}")
    # Output: Items 1 and 3, item 2 skipped
```

### Error Tracking in Streams

```python
async def streaming_with_error_tracking():
    success_count = 0
    error_count = 0
    
    async for result in pipeline(data):
        if result.get("error"):
            error_count += 1
            print(f"Error encountered: {result['error']}")
        else:
            success_count += 1
            print(f"Success: {result}")
        
        # Real-time statistics
        total = success_count + error_count
        success_rate = (success_count / total) * 100 if total > 0 else 0
        print(f"Success rate: {success_rate:.1f}% ({success_count}/{total})")
```