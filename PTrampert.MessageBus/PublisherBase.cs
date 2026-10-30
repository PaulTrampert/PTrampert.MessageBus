using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PTrampert.MessageBus.Transport;

namespace PTrampert.MessageBus;

/// <summary>
/// Resolves the route of each published message, serializes it, and hands the resulting
/// <see cref="OutboundMessage"/>s for each target transport to <see cref="SendAsync"/>.
/// </summary>
internal abstract class PublisherBase : IPublisher
{
    private static readonly IReadOnlyDictionary<string, string?> EmptyHeaders = new Dictionary<string, string?>();

    private readonly MessageRouteResolver _routeResolver;
    private readonly IMessageSerializer _serializer;
    private readonly IReadOnlyList<ITransport> _transports;
    private readonly IReadOnlyDictionary<string, ITransport> _transportsByName;

    protected PublisherBase(
        IEnumerable<ITransport> transports,
        MessageRouteResolver? routeResolver = null,
        IMessageSerializer? serializer = null)
    {
        ArgumentNullException.ThrowIfNull(transports);

        _transports = transports.ToArray();
        if (_transports.Count == 0)
        {
            throw new ArgumentException("At least one transport must be registered.", nameof(transports));
        }

        _transportsByName = BuildTransportLookup(_transports);
        _routeResolver = routeResolver ?? new MessageRouteResolver();
        _serializer = serializer ?? new JsonMessageSerializer();
    }

    public Task PublishAsync<T>(T message, CancellationToken cancellationToken = default)
    {
        return PublishCoreAsync(Single(message), topicOverride: null, brokerOverride: null, cancellationToken: cancellationToken);
    }

    public Task PublishManyAsync<T>(IEnumerable<T> messages, CancellationToken cancellationToken = default)
    {
        return PublishCoreAsync(messages, topicOverride: null, brokerOverride: null, cancellationToken: cancellationToken);
    }

    public Task PublishAsync<T>(T message, string topic, CancellationToken cancellationToken = default)
    {
        return PublishCoreAsync(Single(message), topicOverride: topic, brokerOverride: null, cancellationToken: cancellationToken);
    }

    public Task PublishManyAsync<T>(IEnumerable<T> messages, string topic, CancellationToken cancellationToken = default)
    {
        return PublishCoreAsync(messages, topicOverride: topic, brokerOverride: null, cancellationToken: cancellationToken);
    }

    public Task PublishAsync<T>(T message, string topic, string broker, CancellationToken cancellationToken = default)
    {
        return PublishCoreAsync(Single(message), topicOverride: topic, brokerOverride: broker, cancellationToken: cancellationToken);
    }

    public Task PublishManyAsync<T>(IEnumerable<T> messages, string topic, string broker, CancellationToken cancellationToken = default)
    {
        return PublishCoreAsync(messages, topicOverride: topic, brokerOverride: broker, cancellationToken: cancellationToken);
    }

    private async Task PublishCoreAsync<T>(
        IEnumerable<T> messages,
        string? topicOverride,
        string? brokerOverride,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);

        var resolvedRoute = _routeResolver.Resolve<T>();
        var topic = ChooseTopic(topicOverride, resolvedRoute.Topic);
        var broker = ChooseBroker(brokerOverride, resolvedRoute.Broker);

        var payloads = messages.Select(SerializeMessage).ToArray();
        if (payloads.Length == 0)
        {
            return;
        }

        var targets = ResolveTargets(broker);
        foreach (var target in targets)
        {
            var outbound = payloads
                .Select(payload => new OutboundMessage(
                    Body: payload,
                    Topic: topic,
                    Broker: target.Name,
                    Headers: EmptyHeaders,
                    MessageId: Guid.CreateVersion7(),
                    SentAtUtc: DateTimeOffset.UtcNow))
                .ToArray();

            await SendAsync(target, outbound, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Sends <paramref name="messages"/>, all addressed to <paramref name="target"/>.
    /// </summary>
    protected abstract Task SendAsync(ITransport target, IReadOnlyList<OutboundMessage> messages, CancellationToken cancellationToken);

    private static IEnumerable<T> Single<T>(T message)
    {
        return new[] { message };
    }

    private ReadOnlyMemory<byte> SerializeMessage<T>(T message)
    {
        if (message is null)
        {
            throw new ArgumentException("Message cannot be null.", nameof(message));
        }

        return _serializer.Serialize(message);
    }

    private IReadOnlyList<ITransport> ResolveTargets(string? broker)
    {
        if (broker is null)
        {
            return _transports;
        }

        if (!_transportsByName.TryGetValue(broker, out var target))
        {
            throw new ArgumentException($"Broker '{broker}' is not registered.", nameof(broker));
        }

        return new[] { target };
    }

    private static string ChooseTopic(string? explicitTopic, string defaultTopic)
    {
        if (explicitTopic is null)
        {
            return defaultTopic;
        }

        if (string.IsNullOrWhiteSpace(explicitTopic))
        {
            throw new ArgumentException("Topic cannot be empty or whitespace.", nameof(explicitTopic));
        }

        return explicitTopic;
    }

    private static string? ChooseBroker(string? explicitBroker, string? defaultBroker)
    {
        if (explicitBroker is null)
        {
            return defaultBroker;
        }

        if (string.IsNullOrWhiteSpace(explicitBroker))
        {
            throw new ArgumentException("Broker cannot be empty or whitespace.", nameof(explicitBroker));
        }

        return explicitBroker;
    }

    private static IReadOnlyDictionary<string, ITransport> BuildTransportLookup(IEnumerable<ITransport> transports)
    {
        var lookup = new Dictionary<string, ITransport>();
        foreach (var transport in transports)
        {
            if (string.IsNullOrWhiteSpace(transport.Name))
            {
                throw new ArgumentException("Transport name cannot be empty or whitespace.", nameof(transports));
            }

            if (!lookup.TryAdd(transport.Name, transport))
            {
                throw new ArgumentException($"Duplicate transport name '{transport.Name}' detected.", nameof(transports));
            }
        }

        return lookup;
    }
}
