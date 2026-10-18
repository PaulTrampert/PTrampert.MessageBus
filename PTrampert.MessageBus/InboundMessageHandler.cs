using PTrampert.MessageBus.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace PTrampert.MessageBus;

/// <summary>
/// Default implementation of <see cref="IInboundMessageHandler"/> that deserializes incoming messages
/// and dispatches them to a scoped <see cref="IHandler{T}"/> instance.
/// </summary>
/// <typeparam name="T">The message type handled by this adapter.</typeparam>
public class InboundMessageHandler<T> : IInboundMessageHandler
{
    private readonly IServiceProvider _serviceProvider;
    private readonly Type _handlerType;
    private readonly ILogger<InboundMessageHandler<T>> _logger;
    private readonly IMessageSerializer _serializer;

    /// <inheritdoc/>
    public string Name => _handlerType.Name;

    /// <summary>
    /// Initializes a new instance of <see cref="InboundMessageHandler{T}"/>.
    /// </summary>
    /// <param name="serviceProvider">The service provider used to resolve a scoped handler instance per message.</param>
    /// <param name="handlerType">The concrete type that implements <see cref="IHandler{T}"/>.</param>
    /// <param name="logger">Logger instance for this adapter.</param>
    /// <param name="serializer">
    /// Optional serializer used to deserialize inbound messages.
    /// When <see langword="null"/>, a default <see cref="JsonMessageSerializer"/> is used.
    /// </param>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="handlerType"/> does not implement <see cref="IHandler{T}"/>.
    /// </exception>
    public InboundMessageHandler(IServiceProvider serviceProvider, Type handlerType, ILogger<InboundMessageHandler<T>> logger, IMessageSerializer? serializer = null)
    {
        if (!typeof(IHandler<T>).IsAssignableFrom(handlerType))
        {
            throw new ArgumentException($"Handler {handlerType.FullName} does not implement {typeof(IHandler<T>).FullName}.", nameof(handlerType));
        }
        _serviceProvider = serviceProvider;
        _handlerType = handlerType;
        _logger = logger;
        _serializer = serializer ?? new JsonMessageSerializer();
    }


    /// <inheritdoc/>
    public async Task<AckAction> HandleInboundMessageAsync(InboundMessage message, CancellationToken token)
    {
        IHandler<T> handler;
        MessageContext<T> messageContext;
        await using var scope = _serviceProvider.CreateAsyncScope();
        try
        {
            var messageObj = _serializer.Deserialize<T>(message.Body);
            if (messageObj == null)
            {
                throw new MessageNullException();
            }

            handler = (IHandler<T>)scope.ServiceProvider.GetRequiredService(_handlerType);

            messageContext = new MessageContext<T>(
                message.MessageId,
                message.SentAtUtc,
                message.ReceivedAtUtc,
                message.Topic,
                message.Broker,
                message.DeliveryAttempt,
                message.Headers,
                messageObj);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Unprocessable message: {@Message}", message);
            return AckAction.DeadLetter;
        }

        try
        {
            await handler.HandleAsync(messageContext, token);
        }
        catch (OperationCanceledException oce)
        {
            _logger.LogWarning(oce, "Message processing cancelled: {@MessageContext}", messageContext);
            return AckAction.Retry;
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error processing message: {@MessageContext}", messageContext);
            return AckAction.Retry;
        }
        return AckAction.Ack;
    }
}

/// <summary>
/// Exception thrown when a message deserializes to <see langword="null"/>.
/// </summary>
public class MessageNullException() : Exception("Message deserialized as null");

/// <summary>
/// Internal contract between the transport layer and the message-dispatch pipeline.
/// Implementations deserialize raw bytes and invoke the appropriate <see cref="IHandler{TMessage}"/>.
/// </summary>
public interface IInboundMessageHandler
{
    /// <summary>
    /// Logical name of this handler, used for subscription naming and diagnostics.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Processes a single inbound message and returns the acknowledgement action the transport should take.
    /// </summary>
    /// <param name="message">The raw inbound message delivered by the transport.</param>
    /// <param name="token">Cancellation token to signal when processing should stop.</param>
    /// <returns>
    /// An <see cref="AckAction"/> indicating whether the message should be acknowledged,
    /// retried, or moved to dead-letter storage.
    /// </returns>
    Task<AckAction> HandleInboundMessageAsync(InboundMessage message, CancellationToken token);
}