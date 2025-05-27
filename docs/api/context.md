# Context API

## Context Classes

```{eval-rst}
.. automodule:: conveyor.context
   :members:
   :undoc-members:
   :show-inheritance:
```

## Usage Examples

### Data Sharing Between Tasks

The primary purpose of `PipelineContext` is to share data between tasks during pipeline execution.

```python
from conveyor import single_task, Pipeline, PipelineContext, get_current_context

@single_task
async def collect_metadata(item):
    """First task that collects metadata in shared context."""
    context = get_current_context()
    
    if context:
        # Initialize or update shared statistics
        stats = context.data.get('stats', {'total_items': 0, 'processed_chars': 0})
        stats['total_items'] += 1
        stats['processed_chars'] += len(str(item))
        context.data['stats'] = stats
    
    return item

@single_task
async def process_with_stats(item):
    """Second task that uses shared context data."""
    context = get_current_context()
    
    processed_item = {
        'original': item,
        'processed': item.upper() if isinstance(item, str) else str(item).upper()
    }
    
    if context:
        stats = context.data.get('stats', {})
        processed_item['pipeline_stats'] = {
            'total_processed': stats.get('total_items', 0),
            'total_chars': stats.get('processed_chars', 0),
            'pipeline_id': context.pipeline_id
        }
    
    return processed_item

# Create pipeline
pipeline = collect_metadata | process_with_stats

# Run with initial context data
custom_context = PipelineContext(
    data={'batch_id': 'batch_001', 'user_id': 'user_123'}
)

pipeline_with_context = pipeline.with_context(**custom_context.__dict__)

# Execute
data = ["hello", "world", "conveyor", "pipeline"]
async for result in pipeline_with_context(data):
    print(f"Processed: {result}")
```

### Shared Counters and State

```python
from conveyor import single_task, get_current_context

@single_task
async def increment_counter(item):
    context = get_current_context()
    
    if context:
        # Increment shared counter
        context.data['counter'] = context.data.get('counter', 0) + 1
        counter_value = context.data['counter']
        
        return {
            'value': item,
            'counter': counter_value,
            'pipeline_id': context.pipeline_id[:8] if context.pipeline_id else 'unknown'
        }
    
    return {'value': item, 'counter': -1}

# Create pipeline with initial counter
pipeline = increment_counter
custom_pipeline = pipeline.with_context(data={'counter': 100})

async for result in custom_pipeline([1, 2, 3]):
    print(f"Item: {result['value']}, Counter: {result['counter']}")
    # Item: 1, Counter: 101
    # Item: 2, Counter: 102  
    # Item: 3, Counter: 103
```

### Configuration and Dependencies

Share configuration values and external dependencies:

```python
@single_task
async def process_with_config(item):
    context = get_current_context()
    
    if context:
        api_key = context.data.get('api_key', 'default')
        timeout = context.data.get('timeout', 30)
        # Use configuration in processing
        return f"Processed {item} with key={api_key[:4]}..."
    
    return f"Processed {item} (no config)"

# Set up pipeline with configuration
config_context = PipelineContext(
    data={
        'api_key': 'your-secret-key',
        'timeout': 60,
        'environment': 'production'
    }
)

pipeline = process_with_config.with_context(**config_context.__dict__)
```

## Key Concepts

- **PipelineContext.data**: Dictionary for sharing custom data between tasks in the same pipeline execution
- **get_current_context()**: Function to access the current pipeline context from within tasks
- **pipeline_id**: Unique identifier automatically assigned to each pipeline execution
- **Data persistence**: Context data persists and can be modified throughout the pipeline execution
- **Task isolation**: Each pipeline execution gets its own context instance