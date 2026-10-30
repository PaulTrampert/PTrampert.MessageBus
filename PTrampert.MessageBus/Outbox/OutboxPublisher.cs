using PTrampert.MessageBus.Transport;

namespace PTrampert.MessageBus.Outbox;

internal sealed class OutboxPublisher(
    IOutboxStore store,
    IEnumerable<ITransport> transports,
    MessageRouteResolver? routeResolver = null,
    IMessageSerializer? serializer = null)
    : PublisherBase(transports, routeResolver, serializer), IOutboxPublisher
{
    protected override Task SendAsync(ITransport target, IReadOnlyList<OutboundMessage> messages, CancellationToken cancellationToken)
    {
        return store.AddAsync(messages, cancellationToken);
    }
}
