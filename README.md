# STUFIO.com Framework

A modular FastAPI framework for building scalable modern web applications with integrated database support.

## Features

- **Modular Architecture**: Easily extend functionality through pluggable modules
- **Database Integration**: Built-in support for MongoDB and ClickHouse
- **API Development**: Simplified endpoint creation with dependency injection
- **WebSockets**: Native support for real-time communication
- **Background Tasks**: Celery integration for asynchronous processing
- **Security**: Authentication and authorization patterns built-in
- **Documentation**: Automatic API documentation generation

## Installation

```bash
pip install stufio
```

For development setup:

```bash
git clone https://github.com/stufio-com/stufio.git
cd stufio
pip install -e .
```

## Quick Start

```python
from fastapi import FastAPI
from stufio import registry
from stufio.api import deps
from stufio.db import mongo, clickhouse

# Initialize the application
app = FastAPI(title="Stufio Application")

# Register modules
registry.initialize(app)

# Create an API endpoint
@app.get("/api/hello")
async def hello_world():
    return {"message": "Hello from Stufio!"}

# Connect to databases
@app.on_event("startup")
async def startup():
    await mongo.connect()
    await clickhouse.connect()
```

## Project Structure

```
stufio/
├── api/           # API utilities, dependencies, and endpoint helpers
├── core/          # Core framework components and configuration
├── crud/          # Database operations and repositories
├── db/            # Database connections and utilities
├── models/        # Data models and entity definitions
├── modules/       # Extension modules
├── schemas/       # Pydantic schemas for validation
└── utilities/     # General utility functions
```

## Core Concepts

### Module Registry

Stufio uses a module registry system to manage and integrate extensions:

```python
from stufio import ModuleRegistry, ModuleInterface

# Define a custom module
class CustomModule(ModuleInterface):
    def initialize(self, app):
        # Module initialization logic
        pass

# Register the module
from stufio import registry
registry.register("custom", CustomModule())
```

## Database Integration

Stufio provides easy integration with MongoDB and ClickHouse:

```python
from stufio.db import mongo

async def get_users():
    collection = mongo.get_collection("users")
    return await collection.find().to_list(length=50)
```

## Extensions

Extend Stufio with official modules:

- **stufio-activity**: User activity tracking and rate limiting
- **stufio-auth**: Authentication and authorization
- **stufio-admin**: Admin panel integration
- **stufio-storage**: File storage management

## Documentation

For more detailed documentation, visit [the official documentation](https://docs.stufio.com).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License with Attribution. Copyright (c) 2025 Stufio.com Team.

See LICENSE.txt for full details.