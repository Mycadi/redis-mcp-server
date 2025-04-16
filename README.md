# Redis MCP Server

A Redis Management and Control Protocol (MCP) server implementation using Spring Boot and Spring AI.

## Description

This project implements a Redis MCP server that provides a set of tools for Redis operations. It uses `spring-ai-mcp-server-webmvc-spring-boot-starter` to implement MCP Server-Sent Events (SSE) functionality.

## Prerequisites

- JDK 17 or higher
- Maven 3.6 or higher
- Redis server

## Components

- Spring Boot
- Spring AI
- Spring Data Redis
- Lettuce Redis Client
- Jackson
- spring-ai-mcp-server-webmvc-spring-boot-starter

## Features

- Redis key-value operations (set, get, delete)
- Pattern-based key listing
- Optional key expiration time
- SSE-based MCP implementation

## Configuration

### Server Configuration

The Redis connection can be configured using the `redis.url` system property. Default value is `redis://localhost:6379`.

Example:
```bash
java -Dredis.url=redis://your-redis-host:6379 -jar your-app.jar
```

### Cursor Tool Configuration

To use this MCP server in Cursor, add the following configuration to your Cursor settings:

```json
{
  "redis-mcp-server": {
    "url": "http://localhost:8080/sse",
    "enabled": true
  }
}
```

## Building

```bash
mvn clean package
```

## Running

```bash
java -jar target/redis-mcp-server-{version}.jar
```

## API Endpoints

The server exposes the following MCP tools:

- `set`: Set a Redis key-value pair with optional expiration time
- `get`: Get value from Redis by key
- `delete`: Delete one or multiple keys from Redis
- `list`: List Redis keys matching a pattern

## License

This project is licensed under the MIT License. 