# Installation

Get Conveyor Streaming up and running in your Python environment.

## Requirements

- Python 3.8 or higher
- asyncio support

## Install with pip

```bash
pip install conveyor-streaming
```

## Install with Poetry

```bash
poetry add conveyor-streaming
```

## Development Installation

To install from source for development:

```bash
git clone https://github.com/ju-bezdek/conveyor.git
cd conveyor
pip install -e .
```

Or with Poetry:

```bash
git clone https://github.com/ju-bezdek/conveyor.git
cd conveyor
poetry install
```

## Verify Installation

Test your installation:

```python
import asyncio
from conveyor import single_task

@single_task
async def hello(name: str) -> str:
    return f"Hello, {name}!"

async def main():
    result = await hello("World").collect()
    print(result)  # ['Hello, World!']

if __name__ == "__main__":
    asyncio.run(main())
```

## Optional Dependencies

For enhanced functionality, you may want to install additional packages:

- `aiofiles` - For async file operations
- `aiohttp` - For async HTTP requests
- `asyncpg` - For PostgreSQL async database operations

## Next Steps

- Read the [getting-started guide](getting-started.md)
- Try the [quick-start examples](quick-start.md)
- Explore the [user guide](user-guide/index.md)