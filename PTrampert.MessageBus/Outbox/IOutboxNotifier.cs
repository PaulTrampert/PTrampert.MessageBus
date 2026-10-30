namespace PTrampert.MessageBus.Outbox;

/// <summary>
/// Wakes the outbox dispatcher, so committed messages are sent without waiting for
/// <see cref="BusConfigurator.OutboxPollingInterval"/> to elapse.
/// </summary>
public interface IOutboxNotifier
{
    /// <summary>
    /// Signals that messages have been committed to the <see cref="IOutboxStore"/>.
    /// <see cref="IOutboxStore"/> implementations call this once the consumer commits their unit of work.
    /// </summary>
    void Notify();
}
