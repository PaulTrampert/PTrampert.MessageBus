using PTrampert.MessageBus.Registries;
using PTrampert.MessageBus.Transport;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace PTrampert.MessageBus;

internal class BusService(BusConfigurator busConfigurator, ILogger<BusService> logger) : BackgroundService
{
    private readonly List<ITransportSubscription> _subscriptions = [];
    private readonly HandlerRegistry _handlerRegistry = busConfigurator.HandlerRegistry;
    private readonly TransportRegistry _transportRegistry = busConfigurator.TransportRegistry;

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await SubscribeHandlersAsync(cancellationToken);
        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
            }
            catch (OperationCanceledException e)
            {
                if (e.CancellationToken == stoppingToken)
                {
                    logger.LogInformation("Shutting down BusService");
                }
                else
                {
                    logger.LogError(e, "Unexpected cancellation requested, shutting down BusService");
                }

                break;
            }
        }
    }

    private async Task SubscribeHandlersAsync(CancellationToken stoppingToken)
    {
        foreach (var (route, handlers) in _handlerRegistry.Handlers)
        {
            var handlerList = handlers.ToList();
            IEnumerable<ITransport> transports = route.Broker is not null
                ? _transportRegistry.Transports.TryGetValue(route.Broker, out var t) ? [t] : []
                : _transportRegistry.Transports.Values;

            foreach (var transport in transports)
            foreach (var handler in handlerList)
            {
                var subscription = await transport.SubscribeAsync(
                    busConfigurator,
                    route.Topic,
                    handler,
                    stoppingToken);

                _subscriptions.Add(subscription);
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        foreach (var subscription in _subscriptions)
            await subscription.DisposeAsync();

        await base.StopAsync(cancellationToken);
    }
}