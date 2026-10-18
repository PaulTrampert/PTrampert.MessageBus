using PTrampert.MessageBus.Registries;
using PTrampert.MessageBus.Test.TestMessageTypes;
using PTrampert.MessageBus.Transport;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging;
using Moq;

namespace PTrampert.MessageBus.Test;

[TestFixture]
public sealed class BusServiceTests
{
    private HandlerRegistry _handlerRegistry = null!;
    private TransportRegistry _transportRegistry = null!;
    private BusConfigurator _busConfigurator = null!;
    private ILogger<BusService> _logger = null!;
    private BusService _subject = null!;

    [SetUp]
    public void SetUp()
    {
        _handlerRegistry = CreateHandlerRegistry();
        _transportRegistry = new TransportRegistry();
        _busConfigurator = new BusConfigurator(_handlerRegistry, _transportRegistry, Mock.Of<IServiceProvider>());
        _logger = NullLogger<BusService>.Instance;
        _subject = new BusService(_busConfigurator, _logger);
    }

    [TearDown]
    public void TearDown()
    {
        _subject.Dispose();
    }

    [Test]
    public async Task StartAsync_SubscribesAllHandlersWithMessageRouteTopics()
    {
        _handlerRegistry.RegisterHandler<HandlerA, TestMessage>(topic: "topic-a");
        _handlerRegistry.RegisterHandler<HandlerB, TestMessage>(topic: "topic-a");
        _handlerRegistry.RegisterHandler<HandlerC, TestMessage>(topic: "topic-b", broker: "rabbitmq");

        var rabbitTopics = new List<string>();
        var kafkaTopics = new List<string>();

        var rabbit = CreateTransport("rabbitmq", rabbitTopics);
        var kafka = CreateTransport("kafka", kafkaTopics);
        _transportRegistry.Register(rabbit.Object);
        _transportRegistry.Register(kafka.Object);

        await _subject.StartAsync(CancellationToken.None);
        await Task.Delay(50);
        await _subject.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(rabbitTopics.Count(t => t == "topic-a"), Is.EqualTo(2));
            Assert.That(rabbitTopics.Count(t => t == "topic-b"), Is.EqualTo(1));
            Assert.That(kafkaTopics.Count(t => t == "topic-a"), Is.EqualTo(2));
            Assert.That(kafkaTopics.Count(t => t == "topic-b"), Is.EqualTo(0));
        });
    }

    [Test]
    public async Task StartAsync_PassesBusConfigurationToEverySubscription()
    {
        _handlerRegistry.RegisterHandler<HandlerA, TestMessage>(topic: "topic-a");
        _handlerRegistry.RegisterHandler<HandlerB, TestMessage>(topic: "topic-b");
        _busConfigurator.MaxDeliveryAttempts = 7;

        var configurations = new List<IBusConfiguration>();
        _transportRegistry.Register(CreateTransport("rabbitmq", null, configurations: configurations).Object);

        await _subject.StartAsync(CancellationToken.None);
        await _subject.StopAsync(CancellationToken.None);

        Assert.That(configurations, Has.Count.EqualTo(2));
        Assert.That(configurations, Has.All.SameAs(_busConfigurator));
        Assert.That(configurations[0].MaxDeliveryAttempts, Is.EqualTo(7));
    }

    [Test]
    public async Task StopAsync_DisposesAllSubscriptions()
    {
        _handlerRegistry.RegisterHandler<HandlerA, TestMessage>(topic: "topic-a");
        _handlerRegistry.RegisterHandler<HandlerB, TestMessage>(topic: "topic-a");

        var subscriptions = new List<Mock<ITransportSubscription>>();
        var rabbit = CreateTransport("rabbitmq", null, subscriptions);
        var kafka = CreateTransport("kafka", null, subscriptions);
        _transportRegistry.Register(rabbit.Object);
        _transportRegistry.Register(kafka.Object);

        await _subject.StartAsync(CancellationToken.None);
        await Task.Delay(50);
        await _subject.StopAsync(CancellationToken.None);

        Assert.That(subscriptions, Has.Count.EqualTo(4));
        foreach (var subscription in subscriptions)
        {
            subscription.Verify(s => s.DisposeAsync(), Times.Once);
        }
    }

    [Test]
    public async Task StopAsync_CompletesExecuteTask()
    {
        await _subject.StartAsync(CancellationToken.None);
        await Task.Delay(50);
        await _subject.StopAsync(CancellationToken.None);

        Assert.That(_subject.ExecuteTask, Is.Not.Null);
        Assert.That(_subject.ExecuteTask!.IsCompleted, Is.True);
    }

    [Test]
    public async Task ExecuteTask_DoesNotCompleteUntilServiceIsStopped()
    {
        await _subject.StartAsync(CancellationToken.None);
        await Task.Delay(100);

        Assert.That(_subject.ExecuteTask, Is.Not.Null);
        Assert.That(_subject.ExecuteTask!.IsCompleted, Is.False);

        await _subject.StopAsync(CancellationToken.None);

        await _subject.ExecuteTask.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.That(_subject.ExecuteTask.IsCompleted, Is.True);
    }

    private static HandlerRegistry CreateHandlerRegistry()
    {
        var loggerFactory = new Mock<ILoggerFactory>();
        loggerFactory
            .Setup(f => f.CreateLogger(It.IsAny<string>()))
            .Returns(Mock.Of<ILogger>());

        return new HandlerRegistry(new MessageRouteResolver(), Mock.Of<IServiceProvider>(), loggerFactory.Object);
    }

    private static Mock<ITransport> CreateTransport(
        string name,
        List<string>? topics,
        List<Mock<ITransportSubscription>>? createdSubscriptions = null,
        Action<CancellationToken>? onToken = null,
        List<IBusConfiguration>? configurations = null)
    {
        var transport = new Mock<ITransport>();
        transport.Setup(t => t.Name).Returns(name);
        transport.Setup(t => t.Capabilities).Returns(TransportCapability.None);
        transport
            .Setup(t => t.SubscribeAsync(
                It.IsAny<IBusConfiguration>(),
                It.IsAny<string>(),
                It.IsAny<IInboundMessageHandler>(),
                It.IsAny<CancellationToken>()))
            .Callback<IBusConfiguration, string, IInboundMessageHandler, CancellationToken>((configuration, topic, _, token) =>
            {
                configurations?.Add(configuration);
                topics?.Add(topic);
                onToken?.Invoke(token);
            })
            .Returns(() =>
            {
                var subscription = new Mock<ITransportSubscription>();
                subscription.Setup(s => s.DisposeAsync()).Returns(ValueTask.CompletedTask);
                createdSubscriptions?.Add(subscription);
                return Task.FromResult(subscription.Object);
            });

        return transport;
    }

    private sealed class HandlerA : IHandler<TestMessage>
    {
        public Task HandleAsync(MessageContext<TestMessage> context, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class HandlerB : IHandler<TestMessage>
    {
        public Task HandleAsync(MessageContext<TestMessage> context, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class HandlerC : IHandler<TestMessage>
    {
        public Task HandleAsync(MessageContext<TestMessage> context, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }
}

