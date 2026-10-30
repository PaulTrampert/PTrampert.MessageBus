using PTrampert.MessageBus.Outbox;
using PTrampert.MessageBus.Test.TestMessageTypes;
using PTrampert.MessageBus.Transport;
using Moq;

namespace PTrampert.MessageBus.Test.Outbox;

[TestFixture]
public sealed class OutboxPublisherTests
{
    private Mock<ITransport> _sqsMock = null!;
    private Mock<ITransport> _rabbitMock = null!;
    private List<OutboundMessage> _stored = null!;
    private OutboxPublisher _subject = null!;

    [SetUp]
    public void Setup()
    {
        _sqsMock = new Mock<ITransport>(MockBehavior.Strict);
        _sqsMock.Setup(t => t.Name).Returns("sqs");
        _rabbitMock = new Mock<ITransport>(MockBehavior.Strict);
        _rabbitMock.Setup(t => t.Name).Returns("rabbitmq");

        _stored = [];
        var store = new Mock<IOutboxStore>();
        store.Setup(s => s.AddAsync(It.IsAny<IReadOnlyCollection<OutboundMessage>>(), It.IsAny<CancellationToken>()))
            .Callback<IReadOnlyCollection<OutboundMessage>, CancellationToken>((messages, _) => _stored.AddRange(messages))
            .Returns(Task.CompletedTask);

        _subject = new OutboxPublisher(store.Object, [_sqsMock.Object, _rabbitMock.Object]);
    }

    [Test]
    public async Task PublishAsync_StoresMessageInsteadOfSendingIt()
    {
        // The transports are strict mocks with no send setups, so sending would throw.
        await _subject.PublishAsync(new TopicAndBrokerMessage());

        Assert.That(_stored, Has.Count.EqualTo(1));
        Assert.That(_stored[0].Topic, Is.EqualTo("orders-created"));
        Assert.That(_stored[0].Broker, Is.EqualTo("rabbitmq"));
    }

    [Test]
    public async Task PublishAsync_WithoutBroker_StoresOneMessagePerTransport()
    {
        await _subject.PublishAsync(new TestMessage("a", 1));

        Assert.That(_stored.Select(m => m.Broker), Is.EquivalentTo(new[] { "sqs", "rabbitmq" }));
        Assert.That(_stored.Select(m => m.Topic), Has.All.EqualTo(nameof(TestMessage)));
        Assert.That(_stored.Select(m => m.MessageId).Distinct().Count(), Is.EqualTo(2));
    }

    [Test]
    public async Task PublishManyAsync_StoresEveryMessage()
    {
        await _subject.PublishManyAsync([new TestMessage("a", 1), new TestMessage("b", 2)], "custom-topic", "sqs");

        Assert.That(_stored, Has.Count.EqualTo(2));
        Assert.That(_stored.Select(m => m.Topic), Has.All.EqualTo("custom-topic"));
        Assert.That(_stored.Select(m => m.Broker), Has.All.EqualTo("sqs"));
    }

    [Test]
    public async Task PublishAsync_UnknownBroker_Throws()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => _subject.PublishAsync(new TestMessage("a", 1), "topic", "kafka"));
        Assert.That(_stored, Is.Empty);
    }
}
