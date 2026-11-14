# PTrampert.MessageBus [![NuGet Version](https://img.shields.io/nuget/v/PTrampert.MessageBus.svg?style=flat-square)](https://www.nuget.org/packages/PTrampert.MessageBus)

PTrampert.MessageBus is a lightweight .NET messaging abstraction for publishing messages and handling them through pluggable transports. It provides a simple `IPublisher` API, handler-based message processing, and transport integrations like in-memory and RabbitMQ.

## Installation

Install the core library and the in-memory transport package:

```bash
dotnet add package PTrampert.MessageBus
dotnet add package PTrampert.MessageBus.Transports.InMemory
```

## Basic usage (InMemory)

```csharp
using PTrampert.MessageBus;
using PTrampert.MessageBus.Transports.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddScoped<GreetingHandler>();
builder.Services.AddPTrampertMessageBusInMemoryTransport(configure =>
{
    configure.RegisterHandler<GreetingHandler, GreetingMessage>();
});

using var host = builder.Build();
await host.StartAsync();

var publisher = host.Services.GetRequiredService<IPublisher>();
await publisher.PublishAsync(new GreetingMessage("Hello from PTrampert.MessageBus"));

await host.StopAsync();

public sealed record GreetingMessage(string Text);

public sealed class GreetingHandler : IHandler<GreetingMessage>
{
    public Task HandleAsync(MessageContext<GreetingMessage> context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine(context.Message.Text);
        return Task.CompletedTask;
    }
}
```

The message is routed by message type name by default unless a custom route is configured.

## Transactional outbox (Entity Framework Core)

Publishing straight to a broker after saving to a database can lose the message (the app stops between the two) or send one for changes that were never saved. The outbox avoids both: messages are written to an outbox table **in your own `DbContext`**, so they are committed by your `SaveChangesAsync` in the same transaction as the rest of your changes. A background dispatcher then sends committed messages to their transport and removes them from the table.

```bash
dotnet add package PTrampert.MessageBus.Outbox.EntityFrameworkCore
```

Map the outbox into your `DbContext`, then add a migration to create its table (`MessageBusOutbox` by default):

```csharp
using PTrampert.MessageBus.Outbox.EntityFrameworkCore;

public sealed class OrdersDbContext(DbContextOptions<OrdersDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.AddPTrampertMessageBusOutbox();
    }
}
```

Register the outbox alongside your transport and `DbContext`:

```csharp
builder.Services.AddDbContext<OrdersDbContext>(options => options.UseSqlServer(connectionString));
builder.Services.AddPTrampertMessageBusRabbitMqTransport(configure =>
{
    // Optional: how often to check for messages committed elsewhere, and how many to send at once.
    configure.OutboxPollingInterval = TimeSpan.FromSeconds(5);
    configure.OutboxBatchSize = 100;
});
builder.Services.AddPTrampertMessageBusEntityFrameworkOutbox<OrdersDbContext>();
```

Publish with `IOutboxPublisher` instead of `IPublisher`. Nothing is sent until you save:

```csharp
public sealed class PlaceOrder(OrdersDbContext db, IOutboxPublisher outbox)
{
    public async Task ExecuteAsync(Order order, CancellationToken cancellationToken)
    {
        db.Orders.Add(order);
        await outbox.PublishAsync(new OrderPlaced(order.Id), cancellationToken);
        await db.SaveChangesAsync(cancellationToken); // commits the order and the message together
    }
}
```

Explicit transactions (`db.Database.BeginTransactionAsync()`) work too: a rolled-back message is never sent.

Delivery is at least once. A message can be sent twice if the app stops between sending it and removing it from the outbox, or if several instances of the app dispatch the same outbox table, so make handlers idempotent (for example by recording `MessageContext.Id`). Messages to the same broker are sent in the order they were committed; when a send fails, that broker's remaining messages wait for the next attempt.

To keep the outbox somewhere other than EF Core, implement `IOutboxStore` and register it with `services.AddPTrampertMessageBusOutbox<MyOutboxStore>()`.
