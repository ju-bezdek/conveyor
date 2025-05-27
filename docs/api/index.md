# API Reference

This section provides detailed API documentation for all Conveyor components.

```{toctree}
:maxdepth: 2
:caption: API Documentation

tasks
pipelines
streams
error-handling
context
```

## Overview

The Conveyor API is organized into several key modules:

- **Tasks**: Core task definition and execution
- **Pipelines**: Pipeline construction and management
- **Streams**: Streaming data processing
- **Error Handling**: Error management and retry mechanisms
- **Context**: Execution context and state management

## Quick Reference

### Core Classes

- `Task`: Base class for all processing tasks
- `Pipeline`: Pipeline orchestration and execution
- `Stream`: Streaming data processor
- `Context`: Execution context manager

### Decorators

- `@task`: Decorator to create tasks from functions
- `@pipeline`: Decorator to create pipelines from functions
- `@stream`: Decorator to create streaming processors