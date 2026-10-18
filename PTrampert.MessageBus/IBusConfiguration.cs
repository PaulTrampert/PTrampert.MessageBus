namespace PTrampert.MessageBus;

/// <summary>
/// Read-only view of the transport-independent bus settings configured through <see cref="BusConfigurator"/>.
/// Passed to transports when they subscribe, so they can apply these settings.
/// </summary>
public interface IBusConfiguration
{
    /// <summary>
    /// The maximum number of times a message is delivered to a handler. A message whose handler still fails on
    /// the last attempt is dead-lettered instead of being retried again.
    /// </summary>
    int MaxDeliveryAttempts { get; }
}
