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

Requires Go 1.26.11 or newer. Development and CI use Go 1.27.4 and golangci-lint 2.15.0, pinned in `mise.toml`.

`go get github.com/thalesfsp/queue`

## Contributing

1. Fork
2. Clone
3. Create a branch
4. Make changes following the same standards as the project
5. Run `make ci`
6. Create a merge request

## RabbitMQ subscriptions

Each delivery owns its APM transaction and span; handler and acknowledgement errors are recorded before that tracing lifetime ends. Caller cancellation and context values remain available to the handler.

The callback timeout defaults to five seconds. A positive `queue.SubscribeParams.ContextTimeout` takes precedence over a positive backend `rabbitmq.SubscribeParams.ContextTimeout`; zero or negative values fall back to the backend timeout or the five-second default. `NewSubscribeParams()` leaves the common timeout unset, so editing its backend parameters takes effect unless you explicitly set a positive common timeout.
