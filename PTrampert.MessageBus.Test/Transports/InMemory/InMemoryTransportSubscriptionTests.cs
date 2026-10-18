using System.Collections.Concurrent;
using System.Text;
using PTrampert.MessageBus.Transport;
using PTrampert.MessageBus.Transports.InMemory;
using Microsoft.Extensions.Logging;
using Moq;

namespace PTrampert.MessageBus.Test.Transports.InMemory;

[TestFixture]
public sealed class InMemoryTransportSubscriptionTests
{
    private static readonly IBusConfiguration BusConfiguration = Mock.Of<IBusConfiguration>(c => c.MaxDeliveryAttempts == 5);

    [Test]
    public async Task SubscribeAsync_WhenMessageIsSent_InvokesHandlerWithIncrementedDeliveryAttempt()
    {
        using var loggerFactory = CreateLoggerFactory();
        var transport = new InMemoryTransport(loggerFactory);

        InboundMessage? received = null;
        var receivedSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var subscription = await transport.SubscribeAsync(
            BusConfiguration,
            "orders.created",
            new DelegateInboundMessageHandler((message, _) =>
            {
                received = message;
                receivedSignal.TrySetResult();
                return Task.FromResult(AckAction.Ack);
            }),
            CancellationToken.None);

        await transport.SendAsync(CreateOutboundMessage(topic: "orders.created"));
        await receivedSignal.Task.WaitAsync(TimeSpan.FromSeconds(1));

        Assert.That(received, Is.Not.Null);
        Assert.That(received!.DeliveryAttempt, Is.EqualTo(1));

        await subscription.DisposeAsync();
    }

    [Test]
    public async Task SubscribeAsync_WhenHandlerReturnsRetry_RequeuesMessageWithIncrementedDeliveryAttempt()
    {
        using var loggerFactory = CreateLoggerFactory();
        var transport = new InMemoryTransport(loggerFactory);

        var attempts = new List<int>();
        var secondAttemptSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var subscription = await transport.SubscribeAsync(
            BusConfiguration,
            "orders.created",
            new DelegateInboundMessageHandler((message, _) =>
            {
                attempts.Add(message.DeliveryAttempt);
                if (attempts.Count == 1)
                {
                    return Task.FromResult(AckAction.Retry);
                }

                secondAttemptSignal.TrySetResult();
                return Task.FromResult(AckAction.Ack);
            }),
            CancellationToken.None);

        await transport.SendAsync(CreateOutboundMessage(topic: "orders.created"));
        await secondAttemptSignal.Task.WaitAsync(TimeSpan.FromSeconds(1));

        Assert.That(attempts, Is.EqualTo(new[] { 1, 2 }));

        await subscription.DisposeAsync();
    }

    [Test]
    public async Task SubscribeAsync_WhenHandlerReturnsDeadLetter_LogsError()
    {
        var logs = new ConcurrentQueue<LoggedEvent>();
        using var loggerFactory = CreateLoggerFactory(logs);
        var transport = new InMemoryTransport(loggerFactory);

        var handledSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var subscription = await transport.SubscribeAsync(
            BusConfiguration,
            "orders.created",
            new DelegateInboundMessageHandler((_, _) =>
            {
                handledSignal.TrySetResult();
                return Task.FromResult(AckAction.DeadLetter);
            }),
            CancellationToken.None);

        await transport.SendAsync(CreateOutboundMessage(topic: "orders.created"));
        await handledSignal.Task.WaitAsync(TimeSpan.FromSeconds(1));
        await WaitUntilAsync(
            () => logs.Any(l => l.Level == LogLevel.Error && l.Message.Contains("Dead letter message")),
            TimeSpan.FromSeconds(1));

        var errorLog = logs.SingleOrDefault(l => l.Level == LogLevel.Error && l.Message.Contains("Dead letter message"));
        Assert.That(errorLog, Is.Not.Null);

        await subscription.DisposeAsync();
    }

    [Test]
    public async Task SubscribeAsync_WhenSubscriptionTokenIsCancelled_DoesNotProcessMessages()
    {
        using var loggerFactory = CreateLoggerFactory();
        var transport = new InMemoryTransport(loggerFactory);
        using var cts = new CancellationTokenSource();

        var callbackCount = 0;
        var subscription = await transport.SubscribeAsync(
            BusConfiguration,
            "orders.created",
            new DelegateInboundMessageHandler((_, _) =>
            {
                Interlocked.Increment(ref callbackCount);
                return Task.FromResult(AckAction.Ack);
            }),
            cts.Token);

        cts.Cancel();
        await Task.Delay(100);

        await transport.SendAsync(CreateOutboundMessage(topic: "orders.created"));
        await Task.Delay(100);

        Assert.That(callbackCount, Is.EqualTo(0));

        await subscription.DisposeAsync();
    }

    [Test]
    public async Task DisposeAsync_WhenDisposed_RemovesSubscriptionFromTransport()
    {
        using var loggerFactory = CreateLoggerFactory();
        var transport = new InMemoryTransport(loggerFactory);

        var firstReceivedSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var subscription = await transport.SubscribeAsync(
            BusConfiguration,
            "orders.created",
            new DelegateInboundMessageHandler((_, _) =>
            {
                firstReceivedSignal.TrySetResult();
                return Task.FromResult(AckAction.Ack);
            }),
            CancellationToken.None);

        // Confirm it receives messages before disposal
        await transport.SendAsync(CreateOutboundMessage(topic: "orders.created"));
        await firstReceivedSignal.Task.WaitAsync(TimeSpan.FromSeconds(1));

        await subscription.DisposeAsync();

        var callbackCount = 0;
        var secondReceivedSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscription2 = await transport.SubscribeAsync(
            BusConfiguration,
            "orders.created",
            new DelegateInboundMessageHandler((_, _) =>
            {
                Interlocked.Increment(ref callbackCount);
                secondReceivedSignal.TrySetResult();
                return Task.FromResult(AckAction.Ack);
            }),
            CancellationToken.None);

        await transport.SendAsync(CreateOutboundMessage(topic: "orders.created"));
        await secondReceivedSignal.Task.WaitAsync(TimeSpan.FromSeconds(1));

        // Only the second (still-live) subscription should have received the message
        Assert.That(callbackCount, Is.EqualTo(1));

        await subscription2.DisposeAsync();
    }

    [Test]
    public async Task DisposeAsync_WhenHandlerIsInFlight_WaitsForHandlerToComplete()
    {
        using var loggerFactory = CreateLoggerFactory();
        var transport = new InMemoryTransport(loggerFactory);

        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var subscription = await transport.SubscribeAsync(
            BusConfiguration,
            "orders.created",
            new DelegateInboundMessageHandler(async (_, _) =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task;
                return AckAction.Ack;
            }),
            CancellationToken.None);

        await transport.SendAsync(CreateOutboundMessage(topic: "orders.created"));
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(1));

        var disposeTask = subscription.DisposeAsync().AsTask();

        await Task.Delay(100);
        Assert.That(disposeTask.IsCompleted, Is.False);

        releaseHandler.TrySetResult();
        await disposeTask.WaitAsync(TimeSpan.FromSeconds(1));

        Assert.That(disposeTask.IsCompleted, Is.True);
    }

    private static OutboundMessage CreateOutboundMessage(string topic)
    {
        return new OutboundMessage(
            "{\"id\":1}"u8.ToArray(),
            topic,
            "InMemory",
            new Dictionary<string, string?>(),
            Guid.NewGuid(),
            DateTimeOffset.UtcNow);
    }

    private static ILoggerFactory CreateLoggerFactory(ConcurrentQueue<LoggedEvent>? logs = null)
    {
        return LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Trace);
            if (logs is not null)
            {
                builder.AddProvider(new TestLoggerProvider(logs));
            }
        });
    }

    private static async Task WaitUntilAsync(Func<bool> predicate, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        using var timeoutCts = new CancellationTokenSource(timeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, cancellationToken);
        while (!predicate())
        {
            linkedCts.Token.ThrowIfCancellationRequested();
            await Task.Delay(10, linkedCts.Token);
        }
    }

    private sealed class TestLoggerProvider(ConcurrentQueue<LoggedEvent> logs) : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new TestLogger(logs);
        public void Dispose() { }
    }

    private sealed class TestLogger(ConcurrentQueue<LoggedEvent> logs) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            logs.Enqueue(new LoggedEvent(logLevel, formatter(state, exception), exception));
        }
    }

    private sealed class DelegateInboundMessageHandler(
        Func<InboundMessage, CancellationToken, Task<AckAction>> callback) : IInboundMessageHandler
    {
        public string Name => nameof(DelegateInboundMessageHandler);
        
        public Task<AckAction> HandleInboundMessageAsync(InboundMessage message, CancellationToken token)
        {
            return callback(message, token);
        }
    }

    private sealed record LoggedEvent(LogLevel Level, string Message, Exception? Exception);
}
