using System.Collections.Concurrent;
using System.Threading.Channels;
using PTrampert.MessageBus.Transport;
using Microsoft.Extensions.Logging;

namespace PTrampert.MessageBus.Transports.InMemory;

/// <summary>
/// In-memory <see cref="ITransport"/> implementation that routes messages directly between producers and
/// consumers within the same process. Intended for testing and lightweight single-process scenarios.
/// </summary>
public class InMemoryTransport(ILoggerFactory loggerFactory) : ITransport
{
    private readonly ConcurrentDictionary<string, IEnumerable<InMemoryTransportSubscription>> _subscriptions = new();

    /// <inheritdoc/>
    public string Name => "InMemory";

    /// <inheritdoc/>
    public TransportCapability Capabilities => TransportCapability.None;

    /// <inheritdoc/>
    public async Task SendAsync(OutboundMessage message, CancellationToken cancellationToken = default)
    {
        if (!_subscriptions.TryGetValue(message.Topic, out var subscriptions))
        {
            return;
        }

        await Task.WhenAll(subscriptions.Select(async s =>
        {
            var inboundMessage = new InboundMessage(
                message.Body,
                message.Topic,
                message.Broker,
                message.Headers,
                message.MessageId,
                message.SentAtUtc,
                DateTimeOffset.UtcNow,
                0);
            await s.EnqueueAsync(inboundMessage, cancellationToken);
        }));
    }

    /// <inheritdoc/>
    public async Task SendBatchAsync(IReadOnlyCollection<OutboundMessage> messages, CancellationToken cancellationToken = default)
    {
        await Task.WhenAll(messages.Select(m => SendAsync(m, cancellationToken)));
    }

    /// <inheritdoc/>
    public Task<ITransportSubscription> SubscribeAsync(IBusConfiguration configuration, string topic, IInboundMessageHandler handler, CancellationToken cancellationToken = default)
    {
        var subscription = new InMemoryTransportSubscription(
            configuration,
            $"{Name}_{topic}",
            handler,
            loggerFactory.CreateLogger<InMemoryTransportSubscription>(),
            cancellationToken,
            s => RemoveSubscription(topic, s));
        _subscriptions.AddOrUpdate(topic, _ => [subscription], (_, old) => old.Append(subscription));
        return Task.FromResult<ITransportSubscription>(subscription);
    }

    private void RemoveSubscription(string topic, InMemoryTransportSubscription subscription)
    {
        _subscriptions.AddOrUpdate(
            topic,
            _ => [],
            (_, existing) => existing.Where(s => !ReferenceEquals(s, subscription)).ToList());
    }
}