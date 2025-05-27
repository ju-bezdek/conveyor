# Conveyor Documentation

Welcome to Conveyor, a high-performance Python library for building efficient data processing pipelines with automatic batching, streaming capabilities, and robust error handling.

## Quick Start

```{toctree}
:maxdepth: 1
:caption: Getting Started

installation
quick-start
getting-started
```

```{toctree}
:maxdepth: 1
:caption: User Guide

user-guide/index
user-guide/tasks
user-guide/pipelines
user-guide/streaming
user-guide/context
user-guide/side-inputs
user-guide/error-handling
user-guide/performance
user-guide/best-practices
```

```{toctree}
:maxdepth: 2
:caption: Examples

examples/index
examples/basic-usage
```

```{toctree}
:maxdepth: 1
:caption: API Reference

api/index
```

## Features

- **Automatic Batching**: Efficiently process data in configurable batches
- **Streaming Support**: Handle continuous data streams with backpressure
- **Error Handling**: Robust error management with retry mechanisms
- **Side Inputs**: Support for auxiliary data in processing pipelines
- **High Performance**: Optimized for throughput and low latency
- **Type Safety**: Full type hints and validation support

## Installation

Install Conveyor using pip:

```bash
pip install conveyor-streaming
```

## Basic Example

```python
import conveyor

@conveyor.task
def process_item(item):
    return item * 2

# Process a stream of data
pipeline = conveyor.Pipeline()
pipeline.add_task(process_item)

results = pipeline.run([1, 2, 3, 4, 5])
print(results)  # [2, 4, 6, 8, 10]
```

## Getting Help

- Check the [User Guide](user-guide/index) for detailed documentation
- Browse [Examples](examples/index) for common use cases
- See the [API Reference](api/index) for complete function documentation