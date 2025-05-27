# Context

The context system in Conveyor provides a way to pass shared data and configuration through your pipeline stages.

## Overview

Context allows you to:
- Share configuration across pipeline stages
- Pass external dependencies to tasks
- Maintain state across pipeline execution
- Provide runtime parameters to tasks

## Basic Usage

```python
from conveyor import single_task, Context

@single_task
async def process_with_context(item: str, context: Context) -> str:
    # Access context data
    prefix = context.get("prefix", "default")
    return f"{prefix}: {item}"

# Create pipeline with context
pipeline = process_with_context
context = Context({"prefix": "processed"})

# Use context in pipeline execution
async for result in pipeline(["item1", "item2"], context=context):
    print(result)  # "processed: item1", "processed: item2"
```

## Context Types

### Configuration Context
Store configuration values that tasks need:

```python
context = Context({
    "api_key": "your-api-key",
    "timeout": 30,
    "batch_size": 100
})
```

### Dependency Context
Pass external dependencies like database connections:

```python
context = Context({
    "db": database_connection,
    "cache": redis_client,
    "logger": logger
})
```

## Advanced Usage

### Context Inheritance
Child contexts inherit from parent contexts:

```python
base_context = Context({"env": "production"})
task_context = base_context.child({"task_id": "123"})
# task_context has both "env" and "task_id"
```

### Dynamic Context
Modify context during pipeline execution:

```python
@single_task
async def update_context(item: str, context: Context) -> str:
    context.set("last_processed", item)
    return item
```