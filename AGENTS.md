# AGENTS.md

This file provides guidance for AI coding agents working on the PTrampert.MessageBus repository.

## Project Overview

PTrampert.MessageBus is a lightweight .NET messaging abstraction for publishing messages and handling them through pluggable transports. It provides:

- A simple `IPublisher` API for sending messages.
- Handler-based message processing via `IHandler<TMessage>`.
- Pluggable transport integrations (currently InMemory and RabbitMQ).

## Repository Structure

```
PTrampert.MessageBus/                          # Core library (IPublisher, IHandler, BusService, etc.)
PTrampert.MessageBus.Transports.InMemory/      # In-memory transport implementation
PTrampert.MessageBus.Transports.RabbitMq/      # RabbitMQ transport implementation
PTrampert.MessageBus.Outbox.EntityFrameworkCore/ # EF Core outbox store (lives in the consumer's DbContext)
PTrampert.MessageBus.Test/                     # Unit and end-to-end tests
PTrampert.MessageBus.sln                       # Solution file
global.json                                    # SDK version pin
```

## Building and Testing

```bash
# Build the solution
dotnet build PTrampert.MessageBus.sln

# Run all tests
dotnet test PTrampert.MessageBus.sln
```

The RabbitMQ end-to-end tests automatically spin up a RabbitMQ container using Testcontainers. `PTrampert.MessageBus.Test/rabbitmq.conf` is mounted into the container as its configuration file.

## Key Concepts

- **`IPublisher`** – Injected per-scope; use `PublishAsync<TMessage>` to send a message.
- **`IHandler<TMessage>`** – Implement and register to process messages of a given type. Handlers are scoped.
- **`ITransport`** – Implement to add a new broker/transport backend. Register it through `BusConfigurator`.
- **`MessageRouteAttribute`** – Optionally decorate a message class to override the default route (type name).
- **`BusService`** – A `BackgroundService` that starts subscriptions on host start and tears them down on stop.
- **`IOutboxPublisher`** – An `IPublisher` that writes messages to an `IOutboxStore` (in `PTrampert.MessageBus/Outbox/`) instead of sending them. The store takes part in the consumer's unit of work and never commits it itself; the `OutboxDispatcher` hosted service sends committed messages and marks them dispatched. Register with `AddPTrampertMessageBusOutbox<TStore>()`, or `AddPTrampertMessageBusEntityFrameworkOutbox<TDbContext>()` for EF Core.

## Adding a New Transport

1. Create a new class library project (e.g., `PTrampert.MessageBus.Transports.MyBroker`).
2. Implement `ITransport` (defined in `PTrampert.MessageBus/Transport/`, and optionally `ITransportSender`/`ITransportReceiver`) in your new transport project.
3. Add a `ServiceCollectionExtensions` class with an `AddPTrampertMessageBus<MyBroker>Transport` extension method that calls `services.AddPTrampertMessageBus(configure => ...)` and registers the transport.
4. Add end-to-end tests that extend `EndToEndTestFixture` in `PTrampert.MessageBus.Test/E2E/`.
5. Set `<SymbolPackageFormat>snupkg</SymbolPackageFormat>` in the new `.csproj` to match the other transport projects.

## Coding Conventions

- Target the SDK version pinned in `global.json`.
- Use `sealed` for concrete classes that are not designed for inheritance.
- Use `record` or `sealed record` for message types and value objects.
- Handlers and `IPublisher` are registered as **scoped** services; transports (`ITransportSender`/`ITransportReceiver`) and registries are **singletons**.
- Tests use **NUnit** with `[TestFixture]`/`[Test]`/`[SetUp]`/`[TearDown]` attributes.
- Keep transport-specific code out of the core `PTrampert.MessageBus` library. Likewise, keep persistence-specific code (such as EF Core) out of it: the core library defines `IOutboxStore`, and each store lives in its own project.

## CI / Pull Requests

- PRs must target `main`.
- The CI pipeline (`.github/workflows/dotnet-library.yml`) builds, tests, and publishes NuGet packages on merge to `main`.
- PR titles are validated by `.github/pr-title-checker-config.json` – the title must start with one of: `(MAJOR)`, `(MINOR)`, or `(PATCH)`.
