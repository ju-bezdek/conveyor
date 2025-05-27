# Streams API

## Stream Classes

```{eval-rst}
.. automodule:: conveyor.stream
   :members:
   :undoc-members:
   :show-inheritance:
```

## Usage Examples

### Basic Streaming

```python
from conveyor import Stream, single_task

@single_task
def process_stream_item(item):
    return item.upper()

stream = Stream()
stream.add_task(process_stream_item)

# Process streaming data
for result in stream.process(data_generator()):
    print(result)
```

### Streaming with Backpressure

```python
@batch_task(max_size=100, max_queue_size=1000)
def process_with_backpressure(items):
    # Simulate heavy processing
    time.sleep(0.1)
    return [item.lower() for item in items]

stream = Stream(buffer_size=500)
stream.add_task(process_with_backpressure)
```