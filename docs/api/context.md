# Context API

## Context Classes

```{eval-rst}
.. automodule:: conveyor.context
   :members:
   :undoc-members:
   :show-inheritance:
```

## Usage Examples

### Basic Context Usage

```python
from conveyor import single_task, Pipeline, Context

@single_task
def process_with_context(item, context: Context):
    # Access shared state
    counter = context.get('counter', 0)
    context.set('counter', counter + 1)
    
    # Access configuration
    multiplier = context.config.get('multiplier', 1)
    
    return item * multiplier

pipeline = Pipeline()
pipeline.add_task(process_with_context)

# Run with custom context
context = Context(config={'multiplier': 3})
results = pipeline.run([1, 2, 3], context=context)
```

### Context with Side Inputs

```python
@single_task
def process_with_side_input(item, context: Context):
    # Access side input data
    lookup_table = context.side_inputs['lookup']
    processed_value = lookup_table.get(item, item)
    
    return processed_value

# Setup side inputs
side_inputs = {'lookup': {1: 'one', 2: 'two', 3: 'three'}}
context = Context(side_inputs=side_inputs)

pipeline = Pipeline()
pipeline.add_task(process_with_side_input)
results = pipeline.run([1, 2, 3, 4], context=context)
```