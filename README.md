## queue

A highly flexible and extensible queue interface implementation in Go that provides a unified way to interact with different queue backends.

This package implements a generic queue interface that abstracts away the complexities of working with different queue systems. It provides a consistent API for common and usual operations while supporting various queue backends like RabbitMQ, AWS SQS, Kafka and others. The implementation includes robust error handling, metrics collection, and APM tracing capabilities.

## Features

- Unified Queue Interface: Common interface for multiple queue backends (IQueue)
- Concurrent Operations: Support for both single and multi-queue operations
- Built-in Metrics: Comprehensive metrics tracking for all operations
- APM Integration: Built-in application performance monitoring with distributed tracing
- Extensible Architecture: Easy to implement new queue backends
- Pre/Post Operation Hooks: Customizable hooks for operation lifecycle management
- Type-Safe Operations: Generic type support for type-safe data handling
- RabbitMQ Implementation: Full featured RabbitMQ queue implementation included
- Robust Error Handling: Comprehensive error handling with customer error types
- Logging Support: Integrated logging system with different log levels

## Install

`go get github.com/thalesfsp/queue`

## Contributing

1. Fork
2. Clone
3. Create a branch
4. Make changes following the same standards as the project
5. Run `make ci`
6. Create a merge request