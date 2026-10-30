using System.Collections.Concurrent;
using PTrampert.MessageBus.Transport;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Client.Events;

namespace PTrampert.MessageBus.Transports.RabbitMq;

/// <summary>
/// RabbitMQ <see cref="ITransport"/> implementation. Uses a single shared publisher channel, with publisher
/// confirmations enabled, for sending and creates a dedicated channel per subscription for receiving.
/// </summary>
public sealed class RabbitMqTransport(
    RabbitMqTransportOptions options,
    IConnection connection,
    IRabbitMqMessageMapper messageMapper,
    ILogger<RabbitMqTransport> logger) : ITransport, IAsyncDisposable
{
    private const byte DeclaredExchangeMarker = byte.MinValue;
    private const string QueueTypeArgument = "x-queue-type";
    private const string QuorumQueueType = "quorum";
    private const string DeadLetterExchangeArgument = "x-dead-letter-exchange";
    private const string DeadLetterRoutingKeyArgument = "x-dead-letter-routing-key";
    private static readonly CreateChannelOptions PublisherChannelOptions = new(
        publisherConfirmationsEnabled: true,
        publisherConfirmationTrackingEnabled: true);
    private readonly SemaphoreSlim _publisherChannelLock = new(1, 1);
    private readonly SemaphoreSlim _exchangeDeclareLock = new(1, 1);
    private readonly ConcurrentDictionary<string, byte> _declaredExchanges = new(StringComparer.Ordinal);
    private IChannel? _publisherChannel;
    private int _disposed;

    /// <inheritdoc/>
    public string Name => options.TransportName;

    /// <inheritdoc/>
    public TransportCapability Capabilities => TransportCapability.None;

    /// <inheritdoc/>
    public async Task SendAsync(OutboundMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ThrowIfDisposed();

        var channel = await GetOrCreatePublisherChannelAsync(cancellationToken);
        var properties = messageMapper.MapOutbound(message);

        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await EnsureExchangeDeclaredAsync(channel, message.Topic, cancellationToken);

                // The publisher channel has confirmations enabled, so this completes only once the broker
                // has acknowledged the message, and throws a PublishException if the broker rejects it.
                await channel.BasicPublishAsync(
                    exchange: message.Topic,
                    routingKey: string.Empty,
                    mandatory: false,
                    basicProperties: properties,
                    body: message.Body,
                    cancellationToken: cancellationToken);

                return;
            }
            catch (AlreadyClosedException) when (attempt == 0)
            {
                channel = await GetOrCreatePublisherChannelAsync(cancellationToken);
            }
        }
    }

    /// <inheritdoc/>
    public async Task SendBatchAsync(IReadOnlyCollection<OutboundMessage> messages, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        
        await Task.WhenAll(messages.Select(message => SendAsync(message, cancellationToken)));
    }

    /// <inheritdoc/>
    public Task<ITransportSubscription> SubscribeAsync(IBusConfiguration configuration, string topic,
        IInboundMessageHandler handler, CancellationToken cancellationToken = default)
    {
        return SubscribeInternalAsync(configuration, topic, handler, cancellationToken);
    }

    private async Task<ITransportSubscription> SubscribeInternalAsync(
        IBusConfiguration configuration,
        string topic,
        IInboundMessageHandler handler,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(topic);
        ArgumentNullException.ThrowIfNull(handler);
        
        var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

        await channel.ExchangeDeclareAsync(
            exchange: topic,
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);

        var queueName = $"{Name}.{topic}.{handler.Name}";
        var deadLetterQueueName = $"{queueName}.dead-letter";
        await channel.QueueDeclareAsync(
            queue: deadLetterQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            cancellationToken: cancellationToken);

        // A quorum queue counts deliveries in the x-delivery-count header, which classic queues do not set.
        // Rejected messages are routed through the default exchange to the dead-letter queue.
        await channel.QueueDeclareAsync(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object?>
            {
                [QueueTypeArgument] = QuorumQueueType,
                [DeadLetterExchangeArgument] = string.Empty,
                [DeadLetterRoutingKeyArgument] = deadLetterQueueName,
            },
            cancellationToken: cancellationToken);

        await channel.QueueBindAsync(
            queue: queueName,
            exchange: topic,
            routingKey: string.Empty,
            cancellationToken: cancellationToken);

        var subscriptionCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var subscriptionCancellationToken = subscriptionCancellation.Token;

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, eventArgs) =>
        {
            var inbound = messageMapper.MapInbound(eventArgs, Name);
            AckAction action;
            try
            {
                action = await handler.HandleInboundMessageAsync(inbound, subscriptionCancellationToken);
            }
            catch (OperationCanceledException) when (subscriptionCancellationToken.IsCancellationRequested)
            {
                logger.LogDebug(
                    "Message processing canceled for subscription {TransportName}_{Topic}_{HandlerName}.",
                    Name,
                    topic,
                    handler.Name);
                return;
            }
            catch
            {
                action = AckAction.Retry;
            }

            if (action == AckAction.Retry && inbound.DeliveryAttempt >= configuration.MaxDeliveryAttempts)
            {
                logger.LogError(
                    "Message {MessageId} failed {DeliveryAttempt} delivery attempts on subscription {TransportName}_{Topic}_{HandlerName}; dead-lettering it.",
                    inbound.MessageId,
                    inbound.DeliveryAttempt,
                    Name,
                    topic,
                    handler.Name);
                action = AckAction.DeadLetter;
            }

            try
            {
                switch (action)
                {
                    case AckAction.Ack:
                        await channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false, subscriptionCancellationToken);
                        break;
                    case AckAction.DeadLetter:
                        await channel.BasicNackAsync(eventArgs.DeliveryTag, multiple: false, requeue: false,
                            subscriptionCancellationToken);
                        break;
                    case AckAction.Retry:
                    default:
                        await channel.BasicNackAsync(eventArgs.DeliveryTag, multiple: false, requeue: true,
                            subscriptionCancellationToken);
                        break;
                }
            }
            catch (OperationCanceledException) when (subscriptionCancellationToken.IsCancellationRequested)
            {
                logger.LogDebug(
                    "Message acknowledgement canceled for subscription {TransportName}_{Topic}_{HandlerName}.",
                    Name,
                    topic,
                    handler.Name);
            }
        };

        var consumerTag = await channel.BasicConsumeAsync(
            queue: queueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: cancellationToken);

        return new RabbitMqTransportSubscription(
            $"{Name}_{topic}_{handler.Name}",
            consumerTag,
            channel,
            subscriptionCancellation);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        await _publisherChannelLock.WaitAsync();
        try
        {
            if (_publisherChannel is not null)
            {
                await _publisherChannel.DisposeAsync();
                _publisherChannel = null;
            }
        }
        finally
        {
            _publisherChannelLock.Release();
            _publisherChannelLock.Dispose();
            _exchangeDeclareLock.Dispose();
        }
    }

    private async Task EnsureExchangeDeclaredAsync(IChannel channel, string topic, CancellationToken cancellationToken)
    {
        if (_declaredExchanges.ContainsKey(topic))
        {
            return;
        }

        await _exchangeDeclareLock.WaitAsync(cancellationToken);
        try
        {
            if (!_declaredExchanges.ContainsKey(topic))
            {
                await channel.ExchangeDeclareAsync(
                    exchange: topic,
                    type: ExchangeType.Fanout,
                    durable: true,
                    autoDelete: false,
                    cancellationToken: cancellationToken);
                _declaredExchanges.TryAdd(topic, DeclaredExchangeMarker);
            }
        }
        finally
        {
            _exchangeDeclareLock.Release();
        }
    }

    private async Task<IChannel> GetOrCreatePublisherChannelAsync(CancellationToken cancellationToken)
    {
        var existingChannel = _publisherChannel;
        if (existingChannel is { IsOpen: true })
        {
            return existingChannel;
        }

        await _publisherChannelLock.WaitAsync(cancellationToken);
        try
        {
            existingChannel = _publisherChannel;
            if (existingChannel is { IsOpen: true })
            {
                return existingChannel;
            }

            if (existingChannel is not null)
            {
                await existingChannel.DisposeAsync();
            }

            _publisherChannel = await connection.CreateChannelAsync(PublisherChannelOptions, cancellationToken);
            _declaredExchanges.Clear();
            return _publisherChannel;
        }
        finally
        {
            _publisherChannelLock.Release();
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed == 1, this);
    }
}
