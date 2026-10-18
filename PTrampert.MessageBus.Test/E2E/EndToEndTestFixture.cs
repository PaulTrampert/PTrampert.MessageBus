using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace PTrampert.MessageBus.Test.E2E;

[TestFixture]
public abstract class EndToEndTestFixture
{
    protected const int MaxDeliveryAttempts = 3;
    private const int MaxRecordedAttempts = 20;
    private static readonly TimeSpan DeadLetterTimeout = TimeSpan.FromSeconds(30);

    private IHost _host = null!;

    protected IServiceProvider Services => _host.Services;
    protected IPublisher Publisher;
    protected IServiceScope Scope;

    protected static readonly ConcurrentBag<E2ETestMessage> HandledE2ETestMessages = new();

    /// <summary>The <see cref="MessageContext{TMessage}.DeliveryAttempt"/> of every delivery, keyed by message.</summary>
    protected static readonly ConcurrentDictionary<Guid, ConcurrentQueue<int>> DeliveryAttempts = new();

    protected virtual Task StartExternalDependenciesAsync(CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }

    protected virtual Task StopExternalDependenciesAsync(CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Registers the transport under test. Pass <paramref name="configure"/> on to the transport's
    /// <c>AddPTrampertMessageBus…Transport</c> call so the fixture can configure the bus.
    /// </summary>
    protected abstract void ConfigureServices(IServiceCollection services, Action<BusConfigurator> configure);

    /// <summary>
    /// Returns the messages the transport is currently holding as dead letters, without removing them.
    /// </summary>
    protected abstract Task<IReadOnlyCollection<DeadLetteredMessage>> GetDeadLetteredMessagesAsync(CancellationToken cancellationToken = default);

    [OneTimeSetUp]
    public async Task TestFixtureSetup()
    {
        await StartExternalDependenciesAsync();

        var hostBuilder = Host.CreateDefaultBuilder();

        // A retry loop logs an error on every delivery; the default console logger would buffer all of them.
        hostBuilder.ConfigureLogging(logging => logging.ClearProviders());

        hostBuilder.ConfigureServices(sc =>
        {
            ConfigureServices(sc, bus => bus.MaxDeliveryAttempts = MaxDeliveryAttempts);
            sc.AddScoped<E2ETestMessageHandler>();
            sc.AddScoped<AlwaysFailingMessageHandler>();
            sc.AddScoped<FlakyMessageHandler>();
            sc.AddScoped<StrictMessageHandler>();
        });

        _host = hostBuilder.Build();
        await _host.StartAsync();
    }

    [SetUp]
    public void Setup()
    {
        HandledE2ETestMessages.Clear();
        DeliveryAttempts.Clear();
        Scope = Services.CreateScope();
        Publisher = Scope.ServiceProvider.GetRequiredService<IPublisher>();
    }

    [TearDown]
    public void TearDown()
    {
        Scope.Dispose();
    }

    [OneTimeTearDown]
    public async Task TestFixtureTeardown()
    {
        await _host.StopAsync();

        if (_host is IAsyncDisposable asyncDisposable)
        {
            await asyncDisposable.DisposeAsync();
        }
        else
        {
            _host.Dispose();
        }

        await StopExternalDependenciesAsync();
    }

    [Test]
    public async Task PublishedMessage_IsHandled()
    {
        var message = new E2ETestMessage("Hello World");

        await Publisher.PublishAsync(message);

        E2ETestMessage? handledMessage = null;
        Assert.That(() => HandledE2ETestMessages.TryTake(out handledMessage), Is.True.After(30000, 100));
        Assert.That(handledMessage, Is.EqualTo(message));
    }

    [Test]
    public async Task HandlerAlwaysFails_IsDeadLetteredAfterMaxDeliveryAttempts()
    {
        var message = new AlwaysFailingMessage(Guid.NewGuid());

        await Publisher.PublishAsync(message);

        var deadLettered = await WaitForDeadLetterAsync(AlwaysFailingMessage.Topic, message.Id);
        await Task.Delay(TimeSpan.FromSeconds(1));

        Assert.Multiple(() =>
        {
            Assert.That(deadLettered, Is.True, "A message whose handler always fails should be dead-lettered.");
            Assert.That(AttemptsFor(message.Id), Is.EqualTo(Enumerable.Range(1, MaxDeliveryAttempts)),
                $"The message should be delivered exactly {MaxDeliveryAttempts} times, counting each delivery.");
        });
    }

    [Test]
    public async Task HandlerFailsThenSucceeds_DeliveryAttemptCountsEachDelivery()
    {
        var message = new FlakyMessage(Guid.NewGuid());

        await Publisher.PublishAsync(message);

        Assert.That(() => AttemptsFor(message.Id).Length, Is.EqualTo(MaxDeliveryAttempts).After(30000, 100));
        await Task.Delay(TimeSpan.FromSeconds(1));

        var deadLetters = await GetDeadLetteredMessagesAsync();

        Assert.Multiple(() =>
        {
            Assert.That(AttemptsFor(message.Id), Is.EqualTo(Enumerable.Range(1, MaxDeliveryAttempts)));
            Assert.That(deadLetters.Any(m => m.Topic == FlakyMessage.Topic && m.Body.Contains(message.Id.ToString())), Is.False,
                "A message that succeeds on its last allowed attempt should not be dead-lettered.");
        });
    }

    [Test]
    public async Task MessageFailsToDeserialize_IsDeadLettered()
    {
        var message = new MisshapenMessage(Guid.NewGuid(), Count: "not a number");

        await Publisher.PublishAsync(message);

        var deadLettered = await WaitForDeadLetterAsync(StrictMessage.Topic, message.Id);

        Assert.Multiple(() =>
        {
            Assert.That(deadLettered, Is.True, "A message that fails to deserialize should be dead-lettered.");
            Assert.That(AttemptsFor(message.Id), Is.Empty, "The handler should never see a message that fails to deserialize.");
        });
    }

    private async Task<bool> WaitForDeadLetterAsync(string topic, Guid id)
    {
        var timeoutAt = DateTime.UtcNow + DeadLetterTimeout;
        while (DateTime.UtcNow < timeoutAt)
        {
            var deadLetters = await GetDeadLetteredMessagesAsync();
            if (deadLetters.Any(m => m.Topic == topic && m.Body.Contains(id.ToString())))
            {
                return true;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250));
        }

        return false;
    }

    private static int[] AttemptsFor(Guid id) =>
        DeliveryAttempts.TryGetValue(id, out var attempts) ? attempts.ToArray() : [];

    private static void RecordAttempt(Guid id, int deliveryAttempt)
    {
        // Cap what is recorded, so a message redelivered forever fails the test instead of exhausting memory.
        var attempts = DeliveryAttempts.GetOrAdd(id, _ => new ConcurrentQueue<int>());
        if (attempts.Count < MaxRecordedAttempts)
        {
            attempts.Enqueue(deliveryAttempt);
        }
    }

    /// <summary>A message the transport is holding as a dead letter.</summary>
    /// <param name="Topic">The topic the message was published to.</param>
    /// <param name="Body">The message body, as UTF-8 text.</param>
    protected sealed record DeadLetteredMessage(string Topic, string Body);

    protected sealed record E2ETestMessage(string Value);

    protected sealed class E2ETestMessageHandler : IHandler<E2ETestMessage>
    {
        public Task HandleAsync(MessageContext<E2ETestMessage> context, CancellationToken cancellationToken = default)
        {
            HandledE2ETestMessages.Add(context.Message);
            return Task.CompletedTask;
        }
    }

    [MessageRoute(Topic)]
    protected sealed record AlwaysFailingMessage(Guid Id)
    {
        public const string Topic = "e2e-always-failing";
    }

    protected sealed class AlwaysFailingMessageHandler : IHandler<AlwaysFailingMessage>
    {
        public Task HandleAsync(MessageContext<AlwaysFailingMessage> context, CancellationToken cancellationToken = default)
        {
            RecordAttempt(context.Message.Id, context.DeliveryAttempt);
            throw new InvalidOperationException("This handler always fails.");
        }
    }

    /// <summary>Fails until the last allowed delivery attempt, then succeeds.</summary>
    [MessageRoute(Topic)]
    protected sealed record FlakyMessage(Guid Id)
    {
        public const string Topic = "e2e-flaky";
    }

    protected sealed class FlakyMessageHandler : IHandler<FlakyMessage>
    {
        public Task HandleAsync(MessageContext<FlakyMessage> context, CancellationToken cancellationToken = default)
        {
            RecordAttempt(context.Message.Id, context.DeliveryAttempt);

            // Count calls rather than trusting DeliveryAttempt, so the test still ends if DeliveryAttempt is wrong.
            if (AttemptsFor(context.Message.Id).Length < MaxDeliveryAttempts)
            {
                throw new InvalidOperationException("This handler fails until the last attempt.");
            }

            return Task.CompletedTask;
        }
    }

    [MessageRoute(Topic)]
    protected sealed record StrictMessage(Guid Id, int Count)
    {
        public const string Topic = "e2e-strict";
    }

    protected sealed class StrictMessageHandler : IHandler<StrictMessage>
    {
        public Task HandleAsync(MessageContext<StrictMessage> context, CancellationToken cancellationToken = default)
        {
            RecordAttempt(context.Message.Id, context.DeliveryAttempt);
            return Task.CompletedTask;
        }
    }

    /// <summary>Published to <see cref="StrictMessage"/>'s topic, but <c>Count</c> is not a number, so it cannot be deserialized there.</summary>
    [MessageRoute(StrictMessage.Topic)]
    protected sealed record MisshapenMessage(Guid Id, string Count);
}
