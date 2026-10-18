using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using PTrampert.MessageBus.Transports.RabbitMq;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;

namespace PTrampert.MessageBus.Test.E2E;

public class RabbitMqEndToEndTests : EndToEndTestFixture
{
    private const int ManagementPort = 15672;

#pragma warning disable NUnit1032
    private IContainer _rabbitMqContainer = null!;
#pragma warning restore NUnit1032
    private IConnection? _cachedConnection;
    private static string RabbitMqConfigPath => Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "rabbitmq.conf"));

    protected override async Task StartExternalDependenciesAsync(CancellationToken cancellationToken = default)
    {
        await base.StartExternalDependenciesAsync(cancellationToken);

        _rabbitMqContainer = new ContainerBuilder("rabbitmq:3.13-management")
            .WithBindMount(RabbitMqConfigPath, "/etc/rabbitmq.conf")
            .WithEnvironment("RABBITMQ_CONFIG_FILE", "/etc/rabbitmq.conf")
            .WithPortBinding(5672, true)
            .WithPortBinding(ManagementPort, true)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilInternalTcpPortIsAvailable(5672)
                .UntilCommandIsCompleted("rabbitmq-diagnostics -q check_running"))
            .Build();

        await _rabbitMqContainer.StartAsync(cancellationToken);
        await WaitForRabbitMqReadyAsync(cancellationToken);

        // Pre-initialize the connection asynchronously since DI factories are synchronous.
        var factory = new ConnectionFactory
        {
            HostName = _rabbitMqContainer.Hostname,
            Port = _rabbitMqContainer.GetMappedPublicPort(5672),
            UserName = "messagebus",
            Password = "messagebus",
            VirtualHost = "/",
        };
        _cachedConnection = await factory.CreateConnectionAsync(cancellationToken: cancellationToken);
    }

    private async Task WaitForRabbitMqReadyAsync(CancellationToken cancellationToken = default)
    {
        var factory = new ConnectionFactory
        {
            HostName = _rabbitMqContainer.Hostname,
            Port = _rabbitMqContainer.GetMappedPublicPort(5672),
            UserName = "messagebus",
            Password = "messagebus",
            VirtualHost = "/",
        };

        var timeoutAt = DateTime.UtcNow.AddSeconds(30);
        Exception? lastError = null;

        while (DateTime.UtcNow < timeoutAt)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                await using var connection = await factory.CreateConnectionAsync(cancellationToken: cancellationToken);
                await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

                await channel.ExchangeDeclareAsync(
                    exchange: "ptrampert.messagebus.readiness.probe",
                    type: ExchangeType.Fanout,
                    durable: false,
                    autoDelete: true,
                    cancellationToken: cancellationToken);

                return;
            }
            catch (Exception ex)
            {
                lastError = ex;
                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            }
        }

        throw new TimeoutException("RabbitMQ was not ready to accept AMQP operations within 30 seconds.", lastError);
    }

    /// <summary>
    /// Reads every dead-letter queue without consuming it. Dead-letter queues are not named by any convention the
    /// tests rely on: they are the queues with no consumers, excluding any retry queue (one with a message TTL).
    /// Messages are fetched without being acked, so RabbitMQ puts them back when the channel closes.
    /// </summary>
    protected override async Task<IReadOnlyCollection<DeadLetteredMessage>> GetDeadLetteredMessagesAsync(CancellationToken cancellationToken = default)
    {
        var messages = new List<DeadLetteredMessage>();
        foreach (var queue in await ListQueuesWithoutMessageTtlAsync(cancellationToken))
        {
            await using var channel = await _cachedConnection!.CreateChannelAsync(cancellationToken: cancellationToken);
            var declared = await channel.QueueDeclarePassiveAsync(queue, cancellationToken);
            if (declared.ConsumerCount > 0)
            {
                continue;
            }

            while (await channel.BasicGetAsync(queue, autoAck: false, cancellationToken) is { } result)
            {
                var topic = result.BasicProperties.Headers?.TryGetValue("ptrampert.messagebus.topic", out var raw) == true && raw is byte[] bytes
                    ? Encoding.UTF8.GetString(bytes)
                    : result.Exchange;
                messages.Add(new DeadLetteredMessage(topic, Encoding.UTF8.GetString(result.Body.Span)));
            }
        }

        return messages;
    }

    private async Task<IReadOnlyList<string>> ListQueuesWithoutMessageTtlAsync(CancellationToken cancellationToken)
    {
        using var client = new HttpClient
        {
            BaseAddress = new Uri($"http://{_rabbitMqContainer.Hostname}:{_rabbitMqContainer.GetMappedPublicPort(ManagementPort)}/"),
        };
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue(
            "Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes("messagebus:messagebus")));

        await using var stream = await client.GetStreamAsync("api/queues/%2F?columns=name,arguments", cancellationToken);
        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);

        return document.RootElement.EnumerateArray()
            .Where(queue => !(queue.TryGetProperty("arguments", out var arguments)
                              && arguments.ValueKind == JsonValueKind.Object
                              && arguments.TryGetProperty("x-message-ttl", out _)))
            .Select(queue => queue.GetProperty("name").GetString()!)
            .ToList();
    }

    protected override async Task StopExternalDependenciesAsync(CancellationToken cancellationToken = default)
    {
        if (_cachedConnection is not null)
        {
            _cachedConnection.Dispose();
        }

        await _rabbitMqContainer.DisposeAsync();

        await base.StopExternalDependenciesAsync(cancellationToken);
    }

    protected override void ConfigureServices(IServiceCollection services, Action<BusConfigurator> configure)
    {
        // Provide the pre-initialized async connection.
        services.AddSingleton<IConnection>(_ => _cachedConnection ?? throw new InvalidOperationException("Connection not initialized"));
        services.AddPTrampertMessageBusRabbitMqTransport(configure);
    }
}
