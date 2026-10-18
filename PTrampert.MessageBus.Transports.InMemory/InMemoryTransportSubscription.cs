using System.Threading.Channels;
using PTrampert.MessageBus.Transport;
using Microsoft.Extensions.Logging;

namespace PTrampert.MessageBus.Transports.InMemory;

/// <summary>
/// Represents an active in-memory subscription. Messages enqueued to this subscription are
/// processed sequentially by the associated <see cref="IInboundMessageHandler"/>.
/// </summary>
public class InMemoryTransportSubscription : ITransportSubscription
{
    private readonly IBusConfiguration _busConfiguration;
    
    private readonly Channel<InboundMessage> _messages = Channel.CreateUnbounded<InboundMessage>();

    private readonly ILogger<InMemoryTransportSubscription> _logger;
    
    private readonly Task _processTask;

    private readonly Action<InMemoryTransportSubscription> _onDispose;

    internal InMemoryTransportSubscription(
        IBusConfiguration busConfiguration,
        string name,
        IInboundMessageHandler handler,
        ILogger<InMemoryTransportSubscription> logger, 
        CancellationToken cancellationToken,
        Action<InMemoryTransportSubscription> onDispose
    )
    {
        _busConfiguration = busConfiguration;
        Name = name;
        _logger = logger;
        _onDispose = onDispose;
        _processTask = ProcessAsync(handler, cancellationToken);
    }
    
    /// <inheritdoc/>
    public string Name { get; }

    internal async Task EnqueueAsync(InboundMessage message, CancellationToken cancellationToken)
    {
        await _messages.Writer.WriteAsync(message, cancellationToken);
    }
    
    private async Task ProcessAsync(IInboundMessageHandler handler, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested && await _messages.Reader.WaitToReadAsync(cancellationToken))
            {
                try
                {
                    if (!_messages.Reader.TryRead(out var message))
                    {
                        continue;
                    }
                    message = message with {DeliveryAttempt = message.DeliveryAttempt + 1};
                    var result = await handler.HandleInboundMessageAsync(message, cancellationToken);
                    if (message.DeliveryAttempt < _busConfiguration.MaxDeliveryAttempts && result == AckAction.Retry)
                    {
                        await _messages.Writer.WriteAsync(message, cancellationToken);
                    }
                    else if (result is not AckAction.Ack)
                    {
                        _logger.LogError("Dead letter message: {@Message}", message);
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing message in subscription {SubscriptionName}", Name);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }
    
    /// <inheritdoc/>
    public void Dispose()
    {
        try
        {
            _messages.Writer.Complete();
            _onDispose(this);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Exception thrown disposing of subscription {SubscriptionName}", Name);
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        try
        {
            _messages.Writer.Complete();
            await _processTask;
            _onDispose(this);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Exception thrown disposing async of subscription {SubscriptionName}", Name);
        }
    }
}