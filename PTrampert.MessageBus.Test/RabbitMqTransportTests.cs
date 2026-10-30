using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using PTrampert.MessageBus.Transport;
using PTrampert.MessageBus.Transports.RabbitMq;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace PTrampert.MessageBus.Test;

[TestFixture]
public sealed class RabbitMqTransportTests
{
    private Mock<IConnection> _connection = null!;
    private Mock<IChannel> _channel = null!;
    private CreateChannelOptions? _publisherChannelOptions;
    private RabbitMqTransport _subject = null!;

    [SetUp]
    public void Setup()
    {
        _channel = new Mock<IChannel>();
        _channel.SetupGet(c => c.IsOpen).Returns(true);

        _connection = new Mock<IConnection>();
        _connection
            .Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
            .Callback<CreateChannelOptions?, CancellationToken>((options, _) => _publisherChannelOptions = options)
            .ReturnsAsync(_channel.Object);

        _subject = new RabbitMqTransport(
            new RabbitMqTransportOptions(),
            _connection.Object,
            new RabbitMqMessageMapper(),
            NullLogger<RabbitMqTransport>.Instance);
    }

    [TearDown]
    public async Task TearDown()
    {
        await _subject.DisposeAsync();
    }

    [Test]
    public async Task SendAsync_PublishesOnChannelWithPublisherConfirmationsEnabled()
    {
        await _subject.SendAsync(CreateMessage());

        Assert.That(_publisherChannelOptions, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(_publisherChannelOptions!.PublisherConfirmationsEnabled, Is.True);
            Assert.That(_publisherChannelOptions.PublisherConfirmationTrackingEnabled, Is.True);
        });
    }

    [Test]
    public async Task SendAsync_BrokerRejectsMessage_Throws()
    {
        _channel
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Returns(ValueTask.FromException(new PublishException(1, isReturn: false)));

        await Assert.ThrowsAsync<PublishException>(() => _subject.SendAsync(CreateMessage()));
    }

    private static OutboundMessage CreateMessage() => new(
        Body: Encoding.UTF8.GetBytes("payload"),
        Topic: "orders.created",
        Broker: "rabbitmq",
        Headers: new Dictionary<string, string?>(),
        MessageId: Guid.NewGuid(),
        SentAtUtc: DateTimeOffset.UtcNow);
}
