using PTrampert.MessageBus.Transport;

namespace PTrampert.MessageBus;

internal sealed class DefaultPublisher(
    IEnumerable<ITransport> transports,
    MessageRouteResolver? routeResolver = null,
    IMessageSerializer? serializer = null)
    : PublisherBase(transports, routeResolver, serializer)
{
    protected override Task SendAsync(ITransport target, IReadOnlyList<OutboundMessage> messages, CancellationToken cancellationToken)
    {
        return messages.Count == 1
            ? target.SendAsync(messages[0], cancellationToken)
            : target.SendBatchAsync(messages, cancellationToken);
    }
}
