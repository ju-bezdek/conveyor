# Pipelines API

## Pipeline Classes

```{eval-rst}
.. automodule:: conveyor.pipeline
   :members:
   :undoc-members:
   :show-inheritance:
```

## Usage Examples

### Basic Pipeline

```python
from conveyor import Pipeline, single_task

@single_task
def transform(item):
    return item * 2

pipeline = Pipeline()
pipeline.add_task(transform)
results = pipeline.run([1, 2, 3, 4, 5])
```

### Complex Pipeline

```python
@single_task
def validate(item):
    if item < 0:
        raise ValueError("Negative values not allowed")
    return item

@batch_task(max_size=10)
def process_batch(items):
    return [item ** 2 for item in items]

pipeline = Pipeline()
pipeline.add_task(validate)
pipeline.add_task(process_batch)
results = pipeline.run(range(100))
```