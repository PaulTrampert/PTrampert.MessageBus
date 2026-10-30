using System.Text.Json;
using PTrampert.MessageBus.Transport;
using Microsoft.EntityFrameworkCore;

namespace PTrampert.MessageBus.Outbox.EntityFrameworkCore;

/// <summary>
/// <see cref="IOutboxStore"/> that keeps the outbox in the application's own <typeparamref name="TDbContext"/>.
/// Published messages are added to the context's change tracker, so they are saved by the application's next
/// <see cref="DbContext.SaveChangesAsync(CancellationToken)"/>, in the same transaction as its other changes.
/// </summary>
/// <typeparam name="TDbContext">
/// The application's <see cref="DbContext"/>. Its model must include the outbox, mapped with
/// <see cref="ModelBuilderExtensions.AddPTrampertMessageBusOutbox"/>.
/// </typeparam>
public sealed class EntityFrameworkOutboxStore<TDbContext> : IOutboxStore
    where TDbContext : DbContext
{
    private readonly TDbContext _dbContext;
    private readonly IOutboxNotifier _notifier;
    private bool _hasUnsavedMessages;

    /// <summary>
    /// Initializes a new instance of <see cref="EntityFrameworkOutboxStore{TDbContext}"/>.
    /// </summary>
    /// <param name="dbContext">The application's <see cref="DbContext"/> for the current scope.</param>
    /// <param name="notifier">Wakes the outbox dispatcher once added messages are saved.</param>
    public EntityFrameworkOutboxStore(TDbContext dbContext, IOutboxNotifier notifier)
    {
        _dbContext = dbContext;
        _notifier = notifier;
        _dbContext.SavedChanges += OnSavedChanges;
    }

    private DbSet<OutboxMessage> Messages => _dbContext.Set<OutboxMessage>();

    /// <inheritdoc/>
    public Task AddAsync(IReadOnlyCollection<OutboundMessage> messages, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);

        Messages.AddRange(messages.Select(m => new OutboxMessage
        {
            MessageId = m.MessageId,
            Topic = m.Topic,
            Broker = m.Broker,
            Body = m.Body.ToArray(),
            Headers = JsonSerializer.Serialize(m.Headers),
            SentAtUtc = m.SentAtUtc,
        }));
        _hasUnsavedMessages = true;

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public async Task<IReadOnlyList<OutboundMessage>> GetPendingAsync(int maxCount, CancellationToken cancellationToken = default)
    {
        var messages = await Messages
            .AsNoTracking()
            .OrderBy(m => m.Sequence)
            .Take(maxCount)
            .ToListAsync(cancellationToken);

        return messages
            .Select(m => new OutboundMessage(
                Body: m.Body,
                Topic: m.Topic,
                Broker: m.Broker,
                Headers: JsonSerializer.Deserialize<Dictionary<string, string?>>(m.Headers) ?? [],
                MessageId: m.MessageId,
                SentAtUtc: m.SentAtUtc))
            .ToList();
    }

    /// <inheritdoc/>
    public async Task MarkDispatchedAsync(IReadOnlyCollection<Guid> messageIds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messageIds);

        await Messages
            .Where(m => messageIds.Contains(m.MessageId))
            .ExecuteDeleteAsync(cancellationToken);
    }

    private void OnSavedChanges(object? sender, SavedChangesEventArgs e)
    {
        if (!_hasUnsavedMessages)
        {
            return;
        }

        // Inside an explicit transaction the messages are not committed yet; the dispatcher's next poll finds them.
        _hasUnsavedMessages = false;
        _notifier.Notify();
    }
}
