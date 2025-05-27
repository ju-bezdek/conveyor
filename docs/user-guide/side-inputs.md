# Side Inputs

Side inputs allow tasks to incorporate additional data sources beyond the main pipeline flow. This enables tasks to access configuration, lookup tables, metadata, or other contextual information during processing.

## Basic Side Inputs

### Static Side Inputs

Provide static values that remain constant throughout pipeline execution:

```python
from conveyor import single_task

@single_task
async def enrich_with_metadata(item: dict, version: str, environment: str) -> dict:
    """Enrich items with static metadata"""
    return {
        **item,
        "metadata": {
            "version": version,
            "environment": environment,
            "processed_at": time.time()
        }
    }

# Configure task with static side inputs
enriched_task = enrich_with_metadata.with_inputs(
    version="1.2.0",
    environment="production"
)

# Use in pipeline
pipeline = fetch_data | enriched_task | save_data
```

### Configuration Side Inputs

Pass configuration objects to tasks:

```python
@single_task
async def process_with_config(item: dict, config: dict) -> dict:
    """Process item using configuration settings"""
    
    # Access configuration values
    max_retries = config.get("max_retries", 3)
    timeout = config.get("timeout", 30.0)
    debug_mode = config.get("debug", False)
    
    if debug_mode:
        print(f"Processing item {item.get('id')} with config: {config}")
    
    # Use configuration in processing
    for attempt in range(max_retries):
        try:
            result = await external_api_call(item, timeout=timeout)
            return {"processed": True, "config_used": config, **result}
        except TimeoutError:
            if attempt < max_retries - 1:
                await asyncio.sleep(1.0)
            else:
                raise
    
    return {"error": "max_retries_exceeded", **item}

# Configuration object
app_config = {
    "max_retries": 5,
    "timeout": 15.0,
    "debug": True,
    "api_key": "secret-key-123"
}

# Configure task
configured_task = process_with_config.with_inputs(config=app_config)
pipeline = fetch_items | configured_task | save_results
```

## Dynamic Side Inputs

### Lookup Tables

Use side inputs for data enrichment and lookups:

```python
@single_task
async def enrich_with_user_data(
    transaction: dict, 
    user_lookup: dict, 
    currency_rates: dict
) -> dict:
    """Enrich transaction with user data and currency conversion"""
    
    user_id = transaction["user_id"]
    currency = transaction["currency"]
    amount = transaction["amount"]
    
    # Lookup user information
    user_info = user_lookup.get(user_id, {"name": "Unknown", "tier": "basic"})
    
    # Convert currency to USD
    rate = currency_rates.get(currency, 1.0)
    usd_amount = amount * rate
    
    return {
        **transaction,
        "user_name": user_info["name"],
        "user_tier": user_info["tier"],
        "amount_usd": usd_amount,
        "conversion_rate": rate
    }

async def process_transactions_with_lookups():
    # Load lookup data
    user_lookup = await load_user_lookup_table()
    currency_rates = await fetch_current_exchange_rates()
    
    # Configure task with lookup data
    enrichment_task = enrich_with_user_data.with_inputs(
        user_lookup=user_lookup,
        currency_rates=currency_rates
    )
    
    # Process transactions
    pipeline = fetch_transactions | enrichment_task | save_enriched_transactions
    
    async for result in pipeline(transaction_ids):
        print(f"Processed transaction: {result}")
```

### Streaming Side Inputs

Use other streams as side inputs:

```python
@single_task
async def join_with_stream(
    primary_item: dict, 
    secondary_stream: AsyncIterator[dict]
) -> dict:
    """Join primary item with data from secondary stream"""
    
    # Get next item from secondary stream
    try:
        secondary_item = await anext(secondary_stream)
        return {
            **primary_item,
            "joined_data": secondary_item,
            "join_timestamp": time.time()
        }
    except StopAsyncIteration:
        # No more items in secondary stream
        return {
            **primary_item,
            "joined_data": None,
            "join_error": "secondary_stream_exhausted"
        }

async def streaming_join_example():
    # Create secondary data stream
    async def secondary_data_generator():
        for i in range(100):
            yield {"secondary_id": i, "data": f"secondary_{i}"}
            await asyncio.sleep(0.1)
    
    secondary_stream = secondary_data_generator()
    
    # Configure join task
    join_task = join_with_stream.with_inputs(secondary_stream=secondary_stream)
    
    # Main pipeline
    pipeline = generate_primary_data | join_task | process_joined_data
    
    async for result in pipeline(range(50)):
        print(f"Joined result: {result}")
```

## Advanced Side Input Patterns

### Shared State

Use side inputs to maintain shared state across tasks:

```python
class SharedCounter:
    def __init__(self):
        self.count = 0
        self.lock = asyncio.Lock()
    
    async def increment(self) -> int:
        async with self.lock:
            self.count += 1
            return self.count
    
    async def get_count(self) -> int:
        return self.count

@single_task
async def process_with_counter(item: dict, counter: SharedCounter) -> dict:
    """Process item and track processing count"""
    
    # Increment shared counter
    current_count = await counter.increment()
    
    # Process item
    result = await process_item(item)
    
    return {
        **result,
        "processing_order": current_count,
        "total_processed": await counter.get_count()
    }

async def shared_state_example():
    # Create shared counter
    counter = SharedCounter()
    
    # Configure tasks with shared state
    counting_task = process_with_counter.with_inputs(counter=counter)
    
    pipeline = fetch_data | counting_task | save_data
    
    results = await pipeline(data_items).collect()
    
    final_count = await counter.get_count()
    print(f"Processed {final_count} items total")
```

### Database Connections

Share database connections across tasks:

```python
@single_task
async def save_to_database(item: dict, db_pool: asyncpg.Pool) -> dict:
    """Save item to database using shared connection pool"""
    
    async with db_pool.acquire() as connection:
        await connection.execute(
            "INSERT INTO items (id, data, created_at) VALUES ($1, $2, $3)",
            item["id"], json.dumps(item["data"]), datetime.utcnow()
        )
    
    return {"saved": True, **item}

@batch_task(max_size=100)
async def batch_save_to_database(items: list[dict], db_pool: asyncpg.Pool) -> list[dict]:
    """Batch save items to database"""
    
    async with db_pool.acquire() as connection:
        # Prepare batch insert
        values = [
            (item["id"], json.dumps(item["data"]), datetime.utcnow())
            for item in items
        ]
        
        await connection.executemany(
            "INSERT INTO items (id, data, created_at) VALUES ($1, $2, $3)",
            values
        )
    
    return [{"saved": True, **item} for item in items]

async def database_pipeline_example():
    # Create database connection pool
    db_pool = await asyncpg.create_pool(
        "postgresql://user:password@localhost/database",
        min_size=5,
        max_size=20
    )
    
    try:
        # Configure tasks with database pool
        save_task = save_to_database.with_inputs(db_pool=db_pool)
        batch_save_task = batch_save_to_database.with_inputs(db_pool=db_pool)
        
        # Different pipelines can share the same connection pool
        single_pipeline = process_individual | save_task
        batch_pipeline = process_batch | batch_save_task
        
        # Process data
        await single_pipeline(individual_items).collect()
        await batch_pipeline(batch_items).collect()
        
    finally:
        await db_pool.close()
```

### API Clients

Share HTTP clients and authentication:

```python
@single_task
async def call_external_api(
    item: dict, 
    session: aiohttp.ClientSession,
    auth_token: str,
    base_url: str
) -> dict:
    """Call external API with shared session and auth"""
    
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    
    url = f"{base_url}/api/process"
    
    async with session.post(url, json=item, headers=headers) as response:
        response.raise_for_status()
        api_result = await response.json()
        
        return {
            **item,
            "api_response": api_result,
            "status_code": response.status
        }

async def api_client_example():
    # Create shared HTTP session
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30)
    ) as session:
        
        # Get authentication token
        auth_token = await get_auth_token()
        
        # Configure task with shared resources
        api_task = call_external_api.with_inputs(
            session=session,
            auth_token=auth_token,
            base_url="https://api.example.com"
        )
        
        pipeline = prepare_data | api_task | process_response
        
        async for result in pipeline(input_data):
            print(f"API call result: {result}")
```

## Side Input Composition

### Multiple Side Inputs

Tasks can have multiple side inputs of different types:

```python
@single_task
async def complex_processor(
    item: dict,
    config: dict,
    lookup_table: dict,
    shared_cache: dict,
    metrics_collector: object,
    db_connection: object
) -> dict:
    """Task with multiple side inputs for complex processing"""
    
    # Use configuration
    processing_mode = config.get("mode", "default")
    
    # Lookup enrichment data
    enrichment = lookup_table.get(item["category"], {})
    
    # Check cache
    cache_key = f"processed_{item['id']}"
    if cache_key in shared_cache:
        await metrics_collector.record_cache_hit()
        return shared_cache[cache_key]
    
    # Process item
    if processing_mode == "enhanced":
        result = await enhanced_processing(item, enrichment, db_connection)
    else:
        result = await standard_processing(item, enrichment)
    
    # Update cache and metrics
    shared_cache[cache_key] = result
    await metrics_collector.record_processing_complete()
    
    return result

async def complex_pipeline_example():
    # Prepare all side inputs
    config = load_configuration()
    lookup_table = await load_lookup_data()
    shared_cache = {}
    metrics_collector = MetricsCollector()
    db_connection = await create_db_connection()
    
    try:
        # Configure task with all side inputs
        complex_task = complex_processor.with_inputs(
            config=config,
            lookup_table=lookup_table,
            shared_cache=shared_cache,
            metrics_collector=metrics_collector,
            db_connection=db_connection
        )
        
        pipeline = fetch_items | complex_task | save_results
        
        results = await pipeline(item_ids).collect()
        
        # Access metrics after processing
        print(f"Metrics: {await metrics_collector.get_summary()}")
        
    finally:
        await db_connection.close()
```

### Conditional Side Inputs

Provide different side inputs based on conditions:

```python
@single_task
async def adaptive_processor(
    item: dict,
    primary_config: dict,
    fallback_config: dict = None,
    debug_mode: bool = False
) -> dict:
    """Task that adapts based on available side inputs"""
    
    # Choose configuration based on availability
    if item.get("use_fallback") and fallback_config:
        active_config = fallback_config
        config_type = "fallback"
    else:
        active_config = primary_config
        config_type = "primary"
    
    if debug_mode:
        print(f"Using {config_type} config for item {item.get('id')}")
    
    # Process with selected configuration
    return await process_with_config(item, active_config)

# Configure for different scenarios
normal_task = adaptive_processor.with_inputs(
    primary_config=production_config,
    debug_mode=False
)

robust_task = adaptive_processor.with_inputs(
    primary_config=production_config,
    fallback_config=backup_config,
    debug_mode=True
)
```

## Side Input Best Practices

### Resource Management

Properly manage resources passed as side inputs:

```python
class ManagedResource:
    def __init__(self):
        self.connection = None
        self.is_closed = False
    
    async def __aenter__(self):
        self.connection = await create_connection()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.connection and not self.is_closed:
            await self.connection.close()
            self.is_closed = True
    
    async def execute(self, query: str):
        if self.is_closed:
            raise RuntimeError("Resource is closed")
        return await self.connection.execute(query)

@single_task
async def task_with_managed_resource(item: dict, resource: ManagedResource) -> dict:
    """Task using properly managed resource"""
    result = await resource.execute(f"SELECT * FROM table WHERE id = {item['id']}")
    return {"item": item, "result": result}

async def managed_resource_example():
    async with ManagedResource() as resource:
        # Resource is automatically managed
        managed_task = task_with_managed_resource.with_inputs(resource=resource)
        
        pipeline = fetch_data | managed_task | process_results
        
        results = await pipeline(data_items).collect()
    # Resource is automatically closed here
```

### Thread Safety

Ensure side inputs are thread-safe when used across concurrent tasks:

```python
import threading
from collections import defaultdict

class ThreadSafeCounter:
    def __init__(self):
        self._counts = defaultdict(int)
        self._lock = threading.Lock()
    
    def increment(self, key: str) -> int:
        with self._lock:
            self._counts[key] += 1
            return self._counts[key]
    
    def get_counts(self) -> dict:
        with self._lock:
            return dict(self._counts)

@single_task
def thread_safe_counting_task(item: dict, counter: ThreadSafeCounter) -> dict:
    """Task that safely updates shared counter"""
    category = item.get("category", "unknown")
    count = counter.increment(category)
    
    return {
        **item,
        "category_count": count
    }

# Safe to use across concurrent tasks
counter = ThreadSafeCounter()
counting_task = thread_safe_counting_task.with_inputs(counter=counter)
```

### Memory Efficiency

Be mindful of memory usage with large side inputs:

```python
@single_task
async def memory_efficient_lookup(item: dict, lookup_file: str) -> dict:
    """Use file-based lookup instead of loading everything into memory"""
    
    # Read only what's needed from file
    async with aiofiles.open(lookup_file, 'r') as f:
        async for line in f:
            data = json.loads(line)
            if data["id"] == item["lookup_id"]:
                return {**item, "lookup_data": data}
    
    return {**item, "lookup_data": None}

# Pass file path instead of loaded data
lookup_task = memory_efficient_lookup.with_inputs(
    lookup_file="/path/to/large_lookup_file.jsonl"
)
```

### Side Input Validation

Validate side inputs to catch configuration errors early:

```python
from typing import Dict, Any

def validate_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Validate configuration side input"""
    required_keys = ["api_url", "timeout", "max_retries"]
    
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required config key: {key}")
    
    if config["timeout"] <= 0:
        raise ValueError("Timeout must be positive")
    
    if config["max_retries"] < 0:
        raise ValueError("Max retries cannot be negative")
    
    return config

@single_task
async def validated_task(item: dict, config: dict) -> dict:
    """Task with validated configuration"""
    # Config is guaranteed to be valid
    return await process_with_validated_config(item, config)

# Validate before using
try:
    validated_config = validate_config(raw_config)
    safe_task = validated_task.with_inputs(config=validated_config)
except ValueError as e:
    print(f"Configuration error: {e}")
    raise
```