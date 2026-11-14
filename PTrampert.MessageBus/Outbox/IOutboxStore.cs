using PTrampert.MessageBus.Transport;

namespace PTrampert.MessageBus.Outbox;

/// <summary>
/// Persists outbox messages until the <see cref="IOutboxPublisher"/>'s dispatcher has sent them to their transport.
/// Implementations are registered as scoped services, so they can share the consumer's unit of work (for example a
/// <c>DbContext</c>): messages added through <see cref="AddAsync"/> must only become visible to
/// <see cref="GetPendingAsync"/> once the consumer commits that unit of work.
/// </summary>
public interface IOutboxStore
{
    /// <summary>
    /// Stages messages in the current unit of work. This must not commit the unit of work; the consumer does that,
    /// together with the rest of their changes.
    /// </summary>
    /// <param name="messages">The messages to store, each already addressed to a single broker.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task AddAsync(IReadOnlyCollection<OutboundMessage> messages, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns committed messages that have not been dispatched yet, oldest first.
    /// </summary>
    /// <param name="maxCount">The maximum number of messages to return.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task<IReadOnlyList<OutboundMessage>> GetPendingAsync(int maxCount, CancellationToken cancellationToken = default);

    /// <summary>
    /// Records that messages have been sent, so they are no longer returned by <see cref="GetPendingAsync"/>.
    /// Unlike <see cref="AddAsync"/>, this takes effect immediately.
    /// </summary>
    /// <param name="messageIds">The <see cref="OutboundMessage.MessageId"/>s of the sent messages.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task MarkDispatchedAsync(IReadOnlyCollection<Guid> messageIds, CancellationToken cancellationToken = default);
}
