using PTrampert.MessageBus.Registries;
using PTrampert.MessageBus.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace PTrampert.MessageBus.Outbox;

/// <summary>
/// Sends the messages committed to the <see cref="IOutboxStore"/> to their transports, then marks them dispatched.
/// </summary>
internal sealed class OutboxDispatcher(
    BusConfigurator busConfigurator,
    IServiceScopeFactory scopeFactory,
    OutboxNotifier notifier,
    IHostApplicationLifetime lifetime,
    ILogger<OutboxDispatcher> logger) : BackgroundService
{
    private readonly TransportRegistry _transportRegistry = busConfigurator.TransportRegistry;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            // Subscriptions are set up while the host starts. Sending before then could drop messages on a
            // transport that only delivers to existing subscribers, such as the in-memory transport.
            await WaitForApplicationStartedAsync(stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                bool morePending;
                try
                {
                    morePending = await DispatchPendingAsync(stoppingToken);
                }
                catch (Exception e) when (e is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
                {
                    logger.LogError(e, "Error dispatching outbox messages");
                    morePending = false;
                }

                if (!morePending)
                {
                    await notifier.WaitAsync(busConfigurator.OutboxPollingInterval, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }

        logger.LogInformation("Shutting down OutboxDispatcher");
    }

    /// <summary>
    /// Sends one batch of pending messages.
    /// </summary>
    /// <returns><see langword="true"/> when every message in a full batch was sent, so more may be waiting.</returns>
    internal async Task<bool> DispatchPendingAsync(CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();

        var pending = await store.GetPendingAsync(busConfigurator.OutboxBatchSize, cancellationToken);
        var dispatched = new List<Guid>(pending.Count);

        // Once a send to a broker fails, skip that broker's later messages, so they are not sent out of order.
        var failedBrokers = new HashSet<string>();

        try
        {
            foreach (var message in pending)
            {
                if (failedBrokers.Contains(message.Broker))
                {
                    continue;
                }

                if (!_transportRegistry.Transports.TryGetValue(message.Broker, out var transport))
                {
                    logger.LogError("Outbox message {MessageId} is addressed to unregistered broker {Broker}", message.MessageId, message.Broker);
                    failedBrokers.Add(message.Broker);
                    continue;
                }

                try
                {
                    await transport.SendAsync(message, cancellationToken);
                    dispatched.Add(message.MessageId);
                }
                catch (Exception e) when (e is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                {
                    logger.LogError(e, "Error sending outbox message {MessageId} to broker {Broker}", message.MessageId, message.Broker);
                    failedBrokers.Add(message.Broker);
                }
            }
        }
        finally
        {
            if (dispatched.Count > 0)
            {
                // Not cancellable: the messages have been sent, so record it even while shutting down.
                await store.MarkDispatchedAsync(dispatched, CancellationToken.None);
            }
        }

        return pending.Count == busConfigurator.OutboxBatchSize && failedBrokers.Count == 0;
    }

    private async Task WaitForApplicationStartedAsync(CancellationToken stoppingToken)
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using (lifetime.ApplicationStarted.Register(() => started.TrySetResult()))
        await using (stoppingToken.Register(() => started.TrySetCanceled(stoppingToken)))
        {
            await started.Task;
        }
    }
}
