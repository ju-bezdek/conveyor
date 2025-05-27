# Error Handling API

## Error Handling Classes

```{eval-rst}
.. automodule:: conveyor.error_handling
   :members:
   :undoc-members:
   :show-inheritance:
```

## Usage Examples

### Basic Error Handling

```python
from conveyor import single_task, Pipeline
from conveyor.error_handling import RetryConfig

@single_task(retry_config=RetryConfig(max_retries=3, backoff_factor=2.0))
def unreliable_task(item):
    if random.random() < 0.3:  # 30% failure rate
        raise ValueError("Random failure")
    return item.upper()

pipeline = Pipeline()
pipeline.add_task(unreliable_task)
```

### Custom Error Handling

```python
from conveyor.error_handling import ErrorHandler

class CustomErrorHandler(ErrorHandler):
    def handle_error(self, error, item, context):
        if isinstance(error, ValueError):
            # Log and skip
            logger.warning(f"Skipping item due to error: {error}")
            return None
        else:
            # Re-raise for other errors
            raise error

@single_task(error_handler=CustomErrorHandler())
def process_with_custom_handling(item):
    return validate_and_process(item)
```