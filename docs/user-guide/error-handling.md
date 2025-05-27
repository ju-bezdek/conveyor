# Error Handling

Conveyor provides comprehensive error handling capabilities that allow you to build robust pipelines that can gracefully handle failures at various levels.

## Error Handling Strategies

### Error Actions

Conveyor supports several error handling strategies that can be configured per task:

- **`"fail"`** (default): Re-raise the error, stopping pipeline execution
- **`"skip_item"`**: Skip the failed item and continue processing
- **`"skip_batch"`**: For batch tasks, skip the entire batch if any item fails

```python
@single_task(on_error="skip_item")
async def fault_tolerant_task(item: dict) -> dict:
    """Skip individual items that fail"""
    if item.get("invalid"):
        raise ValueError("Invalid item")
    return {"processed": True, **item}

@batch_task(max_size=5, on_error="skip_batch")
async def strict_batch_task(items: list[dict]) -> list[dict]:
    """Skip entire batch if any item fails"""
    for item in items:
        if item.get("corrupted"):
            raise ValueError("Corrupted item in batch")
    return [{"validated": True, **item} for item in items]
```

### Retry Configuration

Configure automatic retries with exponential backoff:

```python
from conveyor import single_task, RetryConfig

@single_task(
    retry_attempts=5,               # Try up to 5 times total
    retry_delay=1.0,                # Start with 1 second delay
    retry_exponential_backoff=True, # Double delay each retry
    retry_max_delay=30.0,           # Cap delay at 30 seconds
    retry_jitter=True,              # Add random jitter to prevent thundering herd
    on_error="skip_item"            # Skip after all retries exhausted
)
async def unreliable_api_call(item: dict) -> dict:
    """Call external API with comprehensive retry logic"""
    async with aiohttp.ClientSession() as session:
        async with session.post("/api/process", json=item) as response:
            if response.status >= 500:
                raise aiohttp.ClientError(f"Server error: {response.status}")
            elif response.status == 429:
                raise aiohttp.ClientError("Rate limited")
            return await response.json()

# Alternative using RetryConfig object
retry_config = RetryConfig(
    attempts=3,
    delay=0.5,
    exponential_backoff=True,
    max_delay=10.0,
    jitter=True
)

@single_task(retry_config=retry_config, on_error="skip_item")
async def configured_task(item: dict) -> dict:
    # Task implementation
    pass
```

## Custom Error Handlers

Create sophisticated error handling logic with custom error handlers:

```python
from conveyor import ErrorHandler
import logging

class SmartErrorHandler(ErrorHandler):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.error_counts = {}
    
    async def handle_error(
        self, 
        error: Exception, 
        item: any, 
        task_name: str, 
        attempt: int
    ) -> tuple[bool, any]:
        """
        Handle errors with custom logic.
        
        Returns:
            tuple[bool, any]: (continue_processing, replacement_value)
                - continue_processing: True to continue, False to re-raise
                - replacement_value: Value to use instead of failed result
        """
        error_type = type(error).__name__
        self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1
        
        self.logger.warning(
            f"Error in {task_name} (attempt {attempt}): {error_type}: {error}"
        )
        
        # Handle different error types differently
        if isinstance(error, ValueError):
            # Data validation errors - provide default value
            self.logger.info(f"Using default value for invalid item: {item}")
            return True, {"error": str(error), "default": True, "original": item}
        
        elif isinstance(error, aiohttp.ClientError):
            # Network errors - retry logic already handled, skip item
            self.logger.warning(f"Network error after retries, skipping item: {item}")
            return True, None  # Skip this item
        
        elif isinstance(error, TimeoutError):
            # Timeout errors - skip but log for monitoring
            self.logger.error(f"Timeout processing item: {item}")
            return True, {"error": "timeout", "item": item}
        
        else:
            # Unknown errors - re-raise for investigation
            self.logger.error(f"Unknown error type {error_type}, re-raising")
            return False, None

# Usage
smart_handler = SmartErrorHandler()

@single_task(error_handler=smart_handler)
async def resilient_task(item: dict) -> dict:
    # Task that might fail in various ways
    if item.get("invalid_format"):
        raise ValueError("Invalid data format")
    elif item.get("network_issue"):
        raise aiohttp.ClientError("Network unavailable")
    elif item.get("slow"):
        raise TimeoutError("Processing timeout")
    
    return {"processed": True, **item}
```

### Logging Error Handler

Built-in error handler for comprehensive logging:

```python
from conveyor import LoggingErrorHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Use built-in logging error handler
@single_task(
    error_handler=LoggingErrorHandler(
        log_level=logging.WARNING,
        include_traceback=True,
        continue_on_error=True
    ),
    retry_attempts=2
)
async def logged_task(item: dict) -> dict:
    # Task implementation with automatic error logging
    pass
```

## Error Handling Patterns

### Graceful Degradation

Continue processing even when some items fail:

```python
@single_task(on_error="skip_item")
async def fetch_user_data(user_id: str) -> dict | None:
    """Fetch user data, skip if user not found"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"/api/users/{user_id}") as response:
                if response.status == 404:
                    raise ValueError(f"User {user_id} not found")
                response.raise_for_status()
                return await response.json()
    except aiohttp.ClientError as e:
        # This will be caught by error handler and item will be skipped
        raise

@single_task
async def enrich_user_data(user_data: dict) -> dict:
    """Only called for successfully fetched users"""
    # Additional processing for valid users
    return {**user_data, "enriched": True}

async def process_users_gracefully():
    user_ids = ["user1", "user2", "invalid_user", "user4"]
    
    pipeline = fetch_user_data | enrich_user_data
    
    valid_users = []
    async for user in pipeline(user_ids):
        valid_users.append(user)
    
    print(f"Successfully processed {len(valid_users)} out of {len(user_ids)} users")
    # Output: Successfully processed 3 out of 4 users (invalid_user skipped)
```

### Error Aggregation

Collect errors for later analysis:

```python
class ErrorCollectingHandler(ErrorHandler):
    def __init__(self):
        self.errors = []
    
    async def handle_error(self, error: Exception, item: any, task_name: str, attempt: int) -> tuple[bool, any]:
        error_info = {
            "error": str(error),
            "error_type": type(error).__name__,
            "item": item,
            "task_name": task_name,
            "attempt": attempt,
            "timestamp": time.time()
        }
        self.errors.append(error_info)
        
        # Continue processing, skip failed items
        return True, None
    
    def get_error_summary(self) -> dict:
        """Get summary of all errors encountered"""
        if not self.errors:
            return {"total_errors": 0}
        
        error_types = {}
        for error in self.errors:
            error_type = error["error_type"]
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        return {
            "total_errors": len(self.errors),
            "error_types": error_types,
            "first_error": self.errors[0]["timestamp"],
            "last_error": self.errors[-1]["timestamp"]
        }

async def process_with_error_collection():
    error_collector = ErrorCollectingHandler()
    
    @single_task(error_handler=error_collector)
    async def potentially_failing_task(item: dict) -> dict:
        if item.get("fail"):
            raise ValueError("Intentional failure")
        return {"processed": True, **item}
    
    data = [
        {"id": 1, "data": "good"},
        {"id": 2, "fail": True},
        {"id": 3, "data": "good"},
        {"id": 4, "fail": True}
    ]
    
    pipeline = potentially_failing_task
    results = await pipeline(data).collect()
    
    print(f"Processed {len(results)} items successfully")
    print("Error summary:", error_collector.get_error_summary())
```

### Circuit Breaker Pattern

Implement circuit breaker to prevent cascading failures:

```python
class CircuitBreakerHandler(ErrorHandler):
    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.circuit_open = False
    
    async def handle_error(self, error: Exception, item: any, task_name: str, attempt: int) -> tuple[bool, any]:
        current_time = time.time()
        
        # Check if circuit should be reset
        if self.circuit_open and (current_time - self.last_failure_time) > self.reset_timeout:
            self.circuit_open = False
            self.failure_count = 0
            print(f"Circuit breaker reset for {task_name}")
        
        # If circuit is open, fail fast
        if self.circuit_open:
            return True, {"error": "circuit_breaker_open", "item": item}
        
        # Increment failure count
        self.failure_count += 1
        self.last_failure_time = current_time
        
        # Open circuit if threshold exceeded
        if self.failure_count >= self.failure_threshold:
            self.circuit_open = True
            print(f"Circuit breaker opened for {task_name} after {self.failure_count} failures")
        
        # Skip failed item
        return True, {"error": str(error), "item": item}

@single_task(error_handler=CircuitBreakerHandler(failure_threshold=3, reset_timeout=30.0))
async def external_service_call(item: dict) -> dict:
    """Call external service with circuit breaker protection"""
    # Simulate external service that might be down
    if random.random() < 0.3:  # 30% failure rate
        raise aiohttp.ClientError("Service unavailable")
    
    return {"processed": True, **item}
```

## Error Handling in Batch Tasks

### Partial Batch Processing

Handle errors within batches selectively:

```python
@batch_task(max_size=5, on_error="skip_item")  # Skip individual items, not whole batch
async def partial_batch_processor(items: list[dict]) -> list[dict]:
    """Process batch items individually, skipping failures"""
    results = []
    
    for item in items:
        try:
            # Process each item
            if item.get("invalid"):
                raise ValueError(f"Invalid item: {item}")
            
            processed_item = {"processed": True, **item}
            results.append(processed_item)
            
        except Exception as e:
            # Error handler will handle this per item
            raise
    
    return results

@batch_task(max_size=10, on_error="skip_batch")
async def atomic_batch_processor(items: list[dict]) -> list[dict]:
    """All-or-nothing batch processing"""
    # Validate entire batch first
    for item in items:
        if item.get("corrupted"):
            raise ValueError(f"Corrupted item in batch: {item}")
    
    # Process entire batch atomically
    await database.begin_transaction()
    try:
        results = []
        for item in items:
            result = await process_and_save(item)
            results.append(result)
        await database.commit()
        return results
    except Exception:
        await database.rollback()
        raise
```

### Batch Error Recovery

Implement sophisticated batch error handling:

```python
class BatchErrorHandler(ErrorHandler):
    async def handle_error(self, error: Exception, batch: list, task_name: str, attempt: int) -> tuple[bool, any]:
        """Handle batch errors with recovery strategies"""
        
        if isinstance(error, ValueError) and "corrupted" in str(error):
            # Try to filter out corrupted items and retry with clean batch
            clean_batch = [item for item in batch if not item.get("corrupted")]
            
            if len(clean_batch) > 0:
                print(f"Retrying batch with {len(clean_batch)}/{len(batch)} clean items")
                return True, clean_batch  # Retry with filtered batch
            else:
                print("No clean items in batch, skipping entirely")
                return True, []  # Skip entire batch
        
        elif isinstance(error, asyncio.TimeoutError):
            # Split large batches on timeout
            if len(batch) > 1:
                mid = len(batch) // 2
                print(f"Splitting batch of {len(batch)} into smaller batches")
                # Process smaller batches separately
                return True, [batch[:mid], batch[mid:]]
            else:
                # Single item timeout, skip it
                return True, []
        
        # Re-raise unknown errors
        return False, None

@batch_task(max_size=20, error_handler=BatchErrorHandler())
async def smart_batch_processor(items: list[dict]) -> list[dict]:
    """Batch processor with intelligent error recovery"""
    # Simulate various failure modes
    if len(items) > 10:
        raise asyncio.TimeoutError("Batch too large")
    
    for item in items:
        if item.get("corrupted"):
            raise ValueError(f"Corrupted item: {item}")
    
    return [{"processed": True, **item} for item in items]
```

## Pipeline-Level Error Handling

### Error Boundaries

Create error boundaries that isolate failures:

```python
async def error_boundary_example():
    """Demonstrate error isolation between pipeline stages"""
    
    @single_task(on_error="skip_item")
    async def safe_stage_1(item: dict) -> dict:
        if item.get("fail_stage_1"):
            raise ValueError("Stage 1 failure")
        return {"stage_1": True, **item}
    
    @single_task(on_error="skip_item") 
    async def safe_stage_2(item: dict) -> dict:
        if item.get("fail_stage_2"):
            raise ValueError("Stage 2 failure")
        return {"stage_2": True, **item}
    
    # Pipeline with error boundaries at each stage
    pipeline = safe_stage_1 | safe_stage_2
    
    test_data = [
        {"id": 1, "data": "good"},
        {"id": 2, "fail_stage_1": True},  # Fails at stage 1
        {"id": 3, "fail_stage_2": True},  # Fails at stage 2
        {"id": 4, "data": "good"}
    ]
    
    results = await pipeline(test_data).collect()
    print(f"Successfully processed {len(results)} out of {len(test_data)} items")
    # Only items 1 and 4 make it through both stages
```

### Global Error Handling

Handle errors at the pipeline consumption level:

```python
async def pipeline_error_handling():
    """Handle errors during pipeline consumption"""
    
    @single_task  # No error handling configured - errors will propagate
    async def potentially_failing_task(item: dict) -> dict:
        if item.get("critical_error"):
            raise RuntimeError("Critical system error")
        return {"processed": True, **item}
    
    pipeline = potentially_failing_task
    
    data = [
        {"id": 1, "data": "good"},
        {"id": 2, "critical_error": True},
        {"id": 3, "data": "good"}
    ]
    
    try:
        results = []
        async for result in pipeline(data):
            results.append(result)
            print(f"Processed: {result}")
    
    except RuntimeError as e:
        print(f"Pipeline failed with critical error: {e}")
        print(f"Successfully processed {len(results)} items before failure")
        
        # Could implement recovery logic here:
        # - Save partial results
        # - Restart from last checkpoint
        # - Alert administrators
```

## Error Monitoring and Metrics

### Error Rate Monitoring

Track error rates in real-time:

```python
class ErrorMetricsHandler(ErrorHandler):
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.recent_results = []  # Sliding window of success/failure
        self.total_processed = 0
        self.total_errors = 0
    
    async def handle_error(self, error: Exception, item: any, task_name: str, attempt: int) -> tuple[bool, any]:
        self.total_errors += 1
        self.total_processed += 1
        
        # Add to sliding window
        self.recent_results.append(False)  # False = error
        if len(self.recent_results) > self.window_size:
            self.recent_results.pop(0)
        
        # Calculate current error rate
        recent_errors = sum(1 for result in self.recent_results if not result)
        current_error_rate = recent_errors / len(self.recent_results) if self.recent_results else 0
        
        # Alert if error rate is high
        if current_error_rate > 0.1:  # 10% error rate threshold
            print(f"⚠️  High error rate detected: {current_error_rate:.1%} in {task_name}")
        
        return True, None  # Skip failed items
    
    def record_success(self):
        """Call this for successful processing"""
        self.total_processed += 1
        self.recent_results.append(True)  # True = success
        if len(self.recent_results) > self.window_size:
            self.recent_results.pop(0)
    
    def get_metrics(self) -> dict:
        """Get current error metrics"""
        if not self.recent_results:
            return {"error_rate": 0, "recent_samples": 0}
        
        recent_errors = sum(1 for result in self.recent_results if not result)
        return {
            "total_processed": self.total_processed,
            "total_errors": self.total_errors,
            "overall_error_rate": self.total_errors / self.total_processed,
            "recent_error_rate": recent_errors / len(self.recent_results),
            "recent_samples": len(self.recent_results)
        }

# Usage with metrics tracking
metrics_handler = ErrorMetricsHandler()

@single_task(error_handler=metrics_handler)
async def monitored_task(item: dict) -> dict:
    # Your task logic here
    result = {"processed": True, **item}
    metrics_handler.record_success()  # Track successful processing
    return result
```

## Best Practices

### Error Handling Guidelines

1. **Fail Fast for Critical Errors**: Don't catch and ignore system errors
2. **Be Specific with Error Types**: Handle different error types appropriately
3. **Log Comprehensively**: Include context, timestamps, and error details
4. **Monitor Error Rates**: Set up alerts for unusual error patterns
5. **Test Error Scenarios**: Include error cases in your tests

```python
# Good: Specific error handling
@single_task(on_error="skip_item")
async def well_designed_task(item: dict) -> dict:
    try:
        # Validate input
        if not isinstance(item.get("data"), str):
            raise ValueError(f"Invalid data type: {type(item.get('data'))}")
        
        # Process item
        result = await external_api_call(item)
        
        # Validate output
        if not result.get("success"):
            raise ValueError(f"API call failed: {result.get('error')}")
        
        return {"processed": True, **result}
    
    except aiohttp.ClientError as e:
        # Network errors - let retry mechanism handle
        raise
    except ValueError as e:
        # Data validation errors - skip item
        raise
    except Exception as e:
        # Unexpected errors - re-raise for investigation
        logger.error(f"Unexpected error in task: {e}", exc_info=True)
        raise

# Bad: Catching all exceptions without specificity
@single_task(on_error="skip_item")
async def poorly_designed_task(item: dict) -> dict:
    try:
        # Task logic
        pass
    except Exception:
        # This hides important errors and makes debugging difficult
        return {"error": "something went wrong"}
```