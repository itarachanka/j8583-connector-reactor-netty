# J8583 Connector Reactor Netty Boilerplate

Boilerplate project contains ISO8583 connector. This template provides a structured starting point for a payment adapter that serves as the critical middleware translation layer for a bank's core acquiring switch or card networks like Visa and Mastercard.

## Features

- **Production-grade TCP client** that handles a reactive stream of ISO8583 messages
- **Non-blocking I/O architecture** using Reactor Netty for high-throughput ingestion
- **Complete observability stack** with metrics, connection health checks

#### Key Architectural Decisions:
1. **Low Coupling and High Cohesion**: Business logic is decoupled from the transport layer
1. **Reactive Streams for backpressure**: Built-in flow control prevents memory exhaustion
1. **Graceful degradation**: Service continues with reduced functionality rather than failing completely

## Quick start

Prerequisites:

* Git
* Gradle (we recommend version 9.7.1)
* Java 25

Building the J8583 Connector from source:

```
git clone https://github.com/itarachanka/j8583-connector-reactor-netty.git
cd j8583-connector-reactor-netty
gradle clean build

```

The resulting jars can be found in the `build/libs` directory.

Run the live documentation using the command:

```
gradle startDocusaurus
```

## Documentation

The documentation of J8583 Connector is located in the `site/` directory of the source code.

## Additional Links
These additional references should also help you:

* [ISO 8583 wiki page](https://en.wikipedia.org/wiki/ISO_8583)
* [Reactor Netty](https://projectreactor.io/docs/netty/release/reference/about-doc.html)
