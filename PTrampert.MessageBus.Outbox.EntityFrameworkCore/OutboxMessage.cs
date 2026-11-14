namespace PTrampert.MessageBus.Outbox.EntityFrameworkCore;

/// <summary>
/// A row in the outbox table, mapped into the application's <c>DbContext</c> by
/// <see cref="ModelBuilderExtensions.AddPTrampertMessageBusOutbox"/>.
/// </summary>
public sealed class OutboxMessage
{
    /// <summary>Store-generated key that orders messages in the order they were added.</summary>
    public long Sequence { get; set; }

    /// <summary>The message id the message is sent with.</summary>
    public Guid MessageId { get; set; }

    /// <summary>The topic the message is sent to.</summary>
    public string Topic { get; set; } = null!;

    /// <summary>The name of the transport the message is sent with.</summary>
    public string Broker { get; set; } = null!;

    /// <summary>The serialized message.</summary>
    public byte[] Body { get; set; } = null!;

    /// <summary>The message headers, as a JSON object.</summary>
    public string Headers { get; set; } = null!;

    /// <summary>When the message was published to the outbox (UTC).</summary>
    public DateTimeOffset SentAtUtc { get; set; }
}
