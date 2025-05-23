import asyncio
import random
from conveyor import single_task, batch_task, ErrorHandler, RetryConfig, LoggingErrorHandler

# Task that randomly fails
@single_task(on_error="skip_item")
async def unreliable_task(x: int) -> int:
    await asyncio.sleep(0.01)
    if random.random() < 0.3:  # 30% failure rate
        raise ValueError(f"Random failure processing {x}")
    return x * 2

# Batch task that fails on certain conditions
@batch_task(max_size=3, on_error="skip_batch")
async def sensitive_batch_task(batch: list[int]) -> int:
    if any(x < 0 for x in batch):
        raise ValueError("Negative numbers not allowed in batch")
    return sum(batch)

# Task with retry logic
@single_task(
    retry_attempts=3,
    retry_delay=0.1,
    retry_exponential_backoff=True,
    on_error="skip_item"
)
async def task_with_retry(x: int) -> int:
    if random.random() < 0.5:  # 50% failure rate
        raise ValueError(f"Temporary failure processing {x}")
    return x + 100

# Custom error handler
class BusinessErrorHandler(ErrorHandler):
    async def handle_error(self, error: Exception, item, task_name: str, attempt: int) -> tuple[bool, any]:
        if isinstance(error, ValueError) and "business rule" in str(error):
            print(f"Business rule violation in {task_name}: {error}")
            return True, -1  # Continue with sentinel value
        return False, None  # Re-raise other errors

@single_task(error_handler=BusinessErrorHandler())
async def business_task(x: int) -> int:
    if x % 7 == 0:
        raise ValueError("business rule: multiples of 7 not allowed")
    return x * 5

async def main():
    data = [1, 2, -1, 3, 4, 5, 6, 7, 8]
    
    print("1. Skip failing items:")
    pipeline1 = unreliable_task | sensitive_batch_task
    try:
        results1 = await pipeline1(data).collect()
        print(f"Results: {results1}")
    except Exception as e:
        print(f"Pipeline failed: {e}")
    
    print("\n2. Retry with backoff:")
    pipeline2 = task_with_retry
    try:
        results2 = await pipeline2([1, 2, 3]).collect()
        print(f"Results: {results2}")
    except Exception as e:
        print(f"Pipeline failed: {e}")
    
    print("\n3. Custom error handler:")
    pipeline3 = business_task
    try:
        results3 = await pipeline3([1, 2, 7, 14, 21]).collect()
        print(f"Results: {results3}")
    except Exception as e:
        print(f"Pipeline failed: {e}")

    print("\n4. Using LoggingErrorHandler:")
    logging_handler = LoggingErrorHandler(replacement_value=0)
    
    @single_task(error_handler=logging_handler)
    async def failing_task(x: int) -> int:
        if x % 2 == 0:
            raise RuntimeError(f"Even numbers not allowed: {x}")
        return x * 10
    
    pipeline4 = failing_task
    results4 = await pipeline4([1, 2, 3, 4, 5]).collect()
    print(f"Results with logging handler: {results4}")

if __name__ == "__main__":
    asyncio.run(main())