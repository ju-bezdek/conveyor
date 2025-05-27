# Context

The context system in Conveyor provides a way to share data between tasks during pipeline execution.

## Overview

The `PipelineContext` allows you to:
- Share data between tasks in the same pipeline execution
- Maintain state that persists across pipeline stages
- Pass configuration and dependencies to tasks
- Track pipeline metadata like pipeline_id

## Basic Usage

```python
from conveyor import single_task, PipelineContext, get_current_context

@single_task
async def process_with_context(item: str) -> str:
    # Access the current pipeline context
    context = get_current_context()
    
    if context:
        # Access shared data
        prefix = context.data.get("prefix", "default")
        return f"{prefix}: {item}"
    
    return f"default: {item}"

# Create pipeline with context
pipeline = process_with_context

# Configure context with initial data
context = PipelineContext(
    data={"prefix": "processed"}
)

# Use context in pipeline execution
pipeline_with_context = pipeline.with_context(**context.__dict__)
async for result in pipeline_with_context(["item1", "item2"]):
    print(result)  # "processed: item1", "processed: item2"
```

## Data Sharing Patterns

### Shared Counters and Statistics

```python
@single_task
async def count_items(item):
    context = get_current_context()
    
    if context:
        # Update shared counter
        count = context.data.get('total_count', 0) + 1
        context.data['total_count'] = count
        
        return {
            'item': item,
            'count': count
        }
    
    return {'item': item}

# Start with initial count
pipeline = count_items.with_context(data={'total_count': 0})
```

### Configuration Sharing

```python
@single_task
async def process_with_config(item):
    context = get_current_context()
    
    if context:
        api_key = context.data.get('api_key', 'default')
        timeout = context.data.get('timeout', 30)
        
        # Use configuration in processing
        return f"Processed {item} with timeout={timeout}"
    
    return f"Processed {item} (no config)"

# Share configuration across all tasks
config_pipeline = process_with_config.with_context(
    data={
        'api_key': 'your-api-key',
        'timeout': 60,
        'environment': 'production'
    }
)
```

### Multi-Task Data Flow

```python
@single_task
async def collect_metadata(item):
    context = get_current_context()
    
    if context:
        # Initialize shared state
        context.data.setdefault('processed_items', [])
        context.data['processed_items'].append(item)
        
        # Update statistics
        stats = context.data.get('stats', {'count': 0, 'total_length': 0})
        stats['count'] += 1
        stats['total_length'] += len(str(item))
        context.data['stats'] = stats
    
    return item

@single_task  
async def add_summary(item):
    context = get_current_context()
    
    if context:
        stats = context.data.get('stats', {})
        return {
            'item': item,
            'total_processed': stats.get('count', 0),
            'avg_length': stats.get('total_length', 0) / max(stats.get('count', 1), 1)
        }
    
    return {'item': item}

# Create multi-stage pipeline
pipeline = collect_metadata | add_summary
```

## Advanced Usage

### Dynamic Context Updates

```python
@single_task
async def update_context_data(item):
    context = get_current_context()
    
    if context:
        # Modify context for downstream tasks
        context.data['last_processed'] = item
        context.data['processing_time'] = time.time()
        
        # Accumulate results
        results = context.data.get('results', [])
        results.append(f"processed_{item}")
        context.data['results'] = results
    
    return item
```

### Context Data Validation

```python
@single_task
async def validate_and_process(item):
    context = get_current_context()
    
    if not context:
        raise ValueError("No context available")
    
    required_keys = ['api_key', 'environment']
    for key in required_keys:
        if key not in context.data:
            raise ValueError(f"Missing required context key: {key}")
    
    return f"Processed {item} in {context.data['environment']}"
```

### Accessing Pipeline Metadata

```python
@single_task
async def log_pipeline_info(item):
    context = get_current_context()
    
    if context:
        pipeline_id = context.pipeline_id[:8] if context.pipeline_id else 'unknown'
        print(f"Processing {item} in pipeline {pipeline_id}")
        
        # Store pipeline info in results
        return {
            'item': item,
            'pipeline_id': pipeline_id,
            'batch_id': context.data.get('batch_id', 'default')
        }
    
    return item
```