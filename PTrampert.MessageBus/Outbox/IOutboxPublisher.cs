namespace PTrampert.MessageBus.Outbox;

/// <summary>
/// An <see cref="IPublisher"/> that writes messages to the <see cref="IOutboxStore"/> instead of sending them.
/// Messages are sent by a background dispatcher once the consumer commits the unit of work the store takes part in
/// (for example by calling <c>SaveChangesAsync</c> on their <c>DbContext</c>), and are never sent if it is not
/// committed. Delivery is at least once: a message can be sent again if the dispatcher stops between sending it and
/// marking it dispatched.
/// </summary>
public interface IOutboxPublisher : IPublisher
{
}
