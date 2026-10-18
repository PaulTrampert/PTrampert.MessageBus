using System.Collections.Concurrent;
using System.Text;
using PTrampert.MessageBus.Transport;
using PTrampert.MessageBus.Transports.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace PTrampert.MessageBus.Test.E2E;

public class InMemoryEndToEndTests : EndToEndTestFixture
{
    private readonly DeadLetterLogCollector _deadLetters = new();

    protected override void ConfigureServices(IServiceCollection services, Action<BusConfigurator> configure)
    {
        services.AddSingleton<ILoggerProvider>(_deadLetters);
        services.AddPTrampertMessageBusInMemoryTransport(configure);
    }

    protected override Task<IReadOnlyCollection<DeadLetteredMessage>> GetDeadLetteredMessagesAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult<IReadOnlyCollection<DeadLetteredMessage>>(_deadLetters.Messages.ToArray());
    }

    /// <summary>
    /// The in-memory transport keeps a dead letter by logging it, so collect the messages it logs.
    /// </summary>
    private sealed class DeadLetterLogCollector : ILoggerProvider, ILogger
    {
        public ConcurrentQueue<DeadLetteredMessage> Messages { get; } = new();

        public ILogger CreateLogger(string categoryName) =>
            categoryName == typeof(InMemoryTransportSubscription).FullName ? this : NullLoggerInstance;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Error;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (state is not IEnumerable<KeyValuePair<string, object?>> values)
            {
                return;
            }

            foreach (var (key, value) in values)
            {
                if (key.TrimStart('@') == "Message" && value is InboundMessage message)
                {
                    Messages.Enqueue(new DeadLetteredMessage(message.Topic, Encoding.UTF8.GetString(message.Body.Span)));
                }
            }
        }

        public void Dispose() { }

        private static readonly ILogger NullLoggerInstance = Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
    }
}
