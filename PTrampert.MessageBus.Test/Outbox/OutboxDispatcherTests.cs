using System.Collections.Concurrent;
using PTrampert.MessageBus.Outbox;
using PTrampert.MessageBus.Registries;
using PTrampert.MessageBus.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace PTrampert.MessageBus.Test.Outbox;

[TestFixture]
public sealed class OutboxDispatcherTests
{
    private FakeOutboxStore _store = null!;
    private TransportRegistry _transportRegistry = null!;
    private BusConfigurator _busConfigurator = null!;
    private OutboxNotifier _notifier = null!;
    private CancellationTokenSource _applicationStarted = null!;
    private ServiceProvider _serviceProvider = null!;
    private OutboxDispatcher _subject = null!;

    [SetUp]
    public void SetUp()
    {
        _store = new FakeOutboxStore();
        _transportRegistry = new TransportRegistry();
        _busConfigurator = new BusConfigurator(
            new HandlerRegistry(new MessageRouteResolver(), Mock.Of<IServiceProvider>(), NullLoggerFactory.Instance),
            _transportRegistry,
            Mock.Of<IServiceProvider>());
        _notifier = new OutboxNotifier();
        _applicationStarted = new CancellationTokenSource();

        var lifetime = new Mock<IHostApplicationLifetime>();
        lifetime.Setup(l => l.ApplicationStarted).Returns(_applicationStarted.Token);

        _serviceProvider = new ServiceCollection()
            .AddSingleton<IOutboxStore>(_store)
            .BuildServiceProvider();

        _subject = new OutboxDispatcher(
            _busConfigurator,
            _serviceProvider.GetRequiredService<IServiceScopeFactory>(),
            _notifier,
            lifetime.Object,
            NullLogger<OutboxDispatcher>.Instance);
    }

    [TearDown]
    public async Task TearDown()
    {
        _subject.Dispose();
        await _serviceProvider.DisposeAsync();
        _applicationStarted.Dispose();
    }

    [Test]
    public async Task DispatchPendingAsync_SendsPendingMessagesInOrderAndMarksThemDispatched()
    {
        var sent = RegisterTransport("rabbitmq");
        var first = _store.Add("rabbitmq");
        var second = _store.Add("rabbitmq");

        await _subject.DispatchPendingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(sent.Select(m => m.MessageId), Is.EqualTo(new[] { first.MessageId, second.MessageId }));
            Assert.That(_store.Pending, Is.Empty);
        });
    }

    [Test]
    public async Task DispatchPendingAsync_SendFails_KeepsThatBrokersMessagesInOrderAndSendsTheOthers()
    {
        var failing = new Mock<ITransport>();
        failing.Setup(t => t.Name).Returns("rabbitmq");
        failing.Setup(t => t.SendAsync(It.IsAny<OutboundMessage>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Broker unavailable"));
        _transportRegistry.Register(failing.Object);
        var sqsSent = RegisterTransport("sqs");

        var rabbitFirst = _store.Add("rabbitmq");
        var sqs = _store.Add("sqs");
        var rabbitSecond = _store.Add("rabbitmq");

        var morePending = await _subject.DispatchPendingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(morePending, Is.False);
            Assert.That(sqsSent.Select(m => m.MessageId), Is.EqualTo(new[] { sqs.MessageId }));
            Assert.That(_store.Pending.Select(m => m.MessageId), Is.EqualTo(new[] { rabbitFirst.MessageId, rabbitSecond.MessageId }));
            failing.Verify(t => t.SendAsync(It.IsAny<OutboundMessage>(), It.IsAny<CancellationToken>()), Times.Once);
        });
    }

    [Test]
    public async Task DispatchPendingAsync_UnregisteredBroker_KeepsMessagePending()
    {
        var message = _store.Add("kafka");

        await _subject.DispatchPendingAsync(CancellationToken.None);

        Assert.That(_store.Pending.Select(m => m.MessageId), Is.EqualTo(new[] { message.MessageId }));
    }

    [Test]
    public async Task DispatchPendingAsync_SendsAtMostOneBatch_AndReportsWhenItWasFull()
    {
        _busConfigurator.OutboxBatchSize = 2;
        var sent = RegisterTransport("rabbitmq");
        for (var i = 0; i < 3; i++)
        {
            _store.Add("rabbitmq");
        }

        var firstBatchFull = await _subject.DispatchPendingAsync(CancellationToken.None);
        var sentAfterFirstBatch = sent.Count;
        var secondBatchFull = await _subject.DispatchPendingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(firstBatchFull, Is.True);
            Assert.That(sentAfterFirstBatch, Is.EqualTo(2));
            Assert.That(secondBatchFull, Is.False);
            Assert.That(sent, Has.Count.EqualTo(3));
        });
    }

    [Test]
    public async Task ExecuteAsync_DoesNotDispatchUntilTheApplicationHasStarted()
    {
        var sent = RegisterTransport("rabbitmq");
        _store.Add("rabbitmq");

        await _subject.StartAsync(CancellationToken.None);
        await Task.Delay(200);
        var sentBeforeStarted = sent.Count;

        await _applicationStarted.CancelAsync();

        Assert.That(sentBeforeStarted, Is.Zero);
        Assert.That(() => sent.Count, Is.EqualTo(1).After(5000, 50));
        await _subject.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_Notify_DispatchesWithoutWaitingForThePollingInterval()
    {
        _busConfigurator.OutboxPollingInterval = TimeSpan.FromHours(1);
        var sent = RegisterTransport("rabbitmq");
        await _applicationStarted.CancelAsync();
        await _subject.StartAsync(CancellationToken.None);
        await Task.Delay(200);

        _store.Add("rabbitmq");
        _notifier.Notify();

        Assert.That(() => sent.Count, Is.EqualTo(1).After(5000, 50));
        await _subject.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task StopAsync_StopsWhileWaitingForThePollingInterval()
    {
        _busConfigurator.OutboxPollingInterval = TimeSpan.FromHours(1);
        await _applicationStarted.CancelAsync();
        await _subject.StartAsync(CancellationToken.None);
        await Task.Delay(200);

        var stop = _subject.StopAsync(CancellationToken.None);

        Assert.That(await Task.WhenAny(stop, Task.Delay(5000)), Is.SameAs(stop));
    }

    private ConcurrentQueue<OutboundMessage> RegisterTransport(string name)
    {
        var sent = new ConcurrentQueue<OutboundMessage>();
        var transport = new Mock<ITransport>();
        transport.Setup(t => t.Name).Returns(name);
        transport.Setup(t => t.SendAsync(It.IsAny<OutboundMessage>(), It.IsAny<CancellationToken>()))
            .Callback<OutboundMessage, CancellationToken>((message, _) => sent.Enqueue(message))
            .Returns(Task.CompletedTask);
        _transportRegistry.Register(transport.Object);
        return sent;
    }

    private sealed class FakeOutboxStore : IOutboxStore
    {
        private readonly List<OutboundMessage> _messages = [];

        public IReadOnlyList<OutboundMessage> Pending
        {
            get { lock (_messages) return _messages.ToList(); }
        }

        public OutboundMessage Add(string broker)
        {
            var message = new OutboundMessage(
                Body: new byte[] { 1 },
                Topic: "topic",
                Broker: broker,
                Headers: new Dictionary<string, string?>(),
                MessageId: Guid.CreateVersion7(),
                SentAtUtc: DateTimeOffset.UtcNow);
            lock (_messages) _messages.Add(message);
            return message;
        }

        public Task AddAsync(IReadOnlyCollection<OutboundMessage> messages, CancellationToken cancellationToken = default)
        {
            lock (_messages) _messages.AddRange(messages);
            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<OutboundMessage>> GetPendingAsync(int maxCount, CancellationToken cancellationToken = default)
        {
            lock (_messages) return Task.FromResult<IReadOnlyList<OutboundMessage>>(_messages.Take(maxCount).ToList());
        }

        public Task MarkDispatchedAsync(IReadOnlyCollection<Guid> messageIds, CancellationToken cancellationToken = default)
        {
            lock (_messages) _messages.RemoveAll(m => messageIds.Contains(m.MessageId));
            return Task.CompletedTask;
        }
    }
}
