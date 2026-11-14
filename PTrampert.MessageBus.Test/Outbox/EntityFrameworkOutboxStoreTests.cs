using PTrampert.MessageBus.Outbox;
using PTrampert.MessageBus.Outbox.EntityFrameworkCore;
using PTrampert.MessageBus.Transport;
using Microsoft.EntityFrameworkCore;
using Moq;

namespace PTrampert.MessageBus.Test.Outbox;

[TestFixture]
public sealed class EntityFrameworkOutboxStoreTests
{
    private string _connectionString = null!;
    private Mock<IOutboxNotifier> _notifier = null!;

    [SetUp]
    public async Task SetUp()
    {
        _connectionString = OutboxTestDbContext.CreateConnectionString();
        _notifier = new Mock<IOutboxNotifier>();

        await using var dbContext = CreateDbContext();
        await dbContext.Database.EnsureCreatedAsync();
    }

    [TearDown]
    public void TearDown()
    {
        OutboxTestDbContext.DeleteDatabase(_connectionString);
    }

    [Test]
    public async Task AddAsync_DoesNotStoreMessagesUntilTheDbContextIsSaved()
    {
        await using var dbContext = CreateDbContext();
        var subject = CreateStore(dbContext);

        await subject.AddAsync([CreateMessage()]);

        Assert.That(await GetPendingAsync(), Is.Empty);
    }

    [Test]
    public async Task AddAsync_StoresMessagesWithTheDbContextsOtherChanges()
    {
        var message = CreateMessage(new Dictionary<string, string?> { ["key"] = "value", ["empty"] = null });
        var widget = new Widget { Id = Guid.NewGuid(), Name = "widget" };

        await using (var dbContext = CreateDbContext())
        {
            dbContext.Widgets.Add(widget);
            await CreateStore(dbContext).AddAsync([message]);
            await dbContext.SaveChangesAsync();
        }

        var pending = await GetPendingAsync();
        await using var verifyContext = CreateDbContext();
        Assert.Multiple(async () =>
        {
            Assert.That(pending, Has.Count.EqualTo(1));
            Assert.That(pending[0].MessageId, Is.EqualTo(message.MessageId));
            Assert.That(pending[0].Topic, Is.EqualTo(message.Topic));
            Assert.That(pending[0].Broker, Is.EqualTo(message.Broker));
            Assert.That(pending[0].Body.ToArray(), Is.EqualTo(message.Body.ToArray()));
            Assert.That(pending[0].Headers, Is.EquivalentTo(message.Headers));
            Assert.That(pending[0].SentAtUtc, Is.EqualTo(message.SentAtUtc));
            Assert.That(await verifyContext.Widgets.AnyAsync(w => w.Id == widget.Id), Is.True);
        });
    }

    [Test]
    public async Task AddAsync_TransactionRolledBack_DoesNotStoreMessages()
    {
        await using (var dbContext = CreateDbContext())
        {
            await using var transaction = await dbContext.Database.BeginTransactionAsync();
            await CreateStore(dbContext).AddAsync([CreateMessage()]);
            await dbContext.SaveChangesAsync();
            await transaction.RollbackAsync();
        }

        Assert.That(await GetPendingAsync(), Is.Empty);
    }

    [Test]
    public async Task SaveChanges_NotifiesOnlyWhenMessagesWereAdded()
    {
        await using var dbContext = CreateDbContext();
        var subject = CreateStore(dbContext);

        dbContext.Widgets.Add(new Widget { Id = Guid.NewGuid(), Name = "without messages" });
        await dbContext.SaveChangesAsync();
        _notifier.Verify(n => n.Notify(), Times.Never);

        await subject.AddAsync([CreateMessage()]);
        _notifier.Verify(n => n.Notify(), Times.Never);

        await dbContext.SaveChangesAsync();
        _notifier.Verify(n => n.Notify(), Times.Once);
    }

    [Test]
    public async Task GetPendingAsync_ReturnsAtMostMaxCountMessagesInTheOrderTheyWereAdded()
    {
        var messages = Enumerable.Range(0, 3).Select(_ => CreateMessage()).ToList();
        await using (var dbContext = CreateDbContext())
        {
            var store = CreateStore(dbContext);
            foreach (var message in messages)
            {
                await store.AddAsync([message]);
                await dbContext.SaveChangesAsync();
            }
        }

        var pending = await GetPendingAsync(maxCount: 2);

        Assert.That(pending.Select(m => m.MessageId), Is.EqualTo(messages.Take(2).Select(m => m.MessageId)));
    }

    [Test]
    public async Task MarkDispatchedAsync_RemovesOnlyTheGivenMessages()
    {
        var dispatched = CreateMessage();
        var remaining = CreateMessage();
        await using (var dbContext = CreateDbContext())
        {
            await CreateStore(dbContext).AddAsync([dispatched, remaining]);
            await dbContext.SaveChangesAsync();
        }

        await using (var dbContext = CreateDbContext())
        {
            await CreateStore(dbContext).MarkDispatchedAsync([dispatched.MessageId]);
        }

        var pending = await GetPendingAsync();
        Assert.That(pending.Select(m => m.MessageId), Is.EqualTo(new[] { remaining.MessageId }));
    }

    private async Task<IReadOnlyList<OutboundMessage>> GetPendingAsync(int maxCount = 100)
    {
        await using var dbContext = CreateDbContext();
        return await CreateStore(dbContext).GetPendingAsync(maxCount);
    }

    private EntityFrameworkOutboxStore<OutboxTestDbContext> CreateStore(OutboxTestDbContext dbContext) =>
        new(dbContext, _notifier.Object);

    private OutboxTestDbContext CreateDbContext() =>
        new(new DbContextOptionsBuilder<OutboxTestDbContext>().UseSqlite(_connectionString).Options);

    private static OutboundMessage CreateMessage(IReadOnlyDictionary<string, string?>? headers = null) =>
        new(
            Body: Guid.NewGuid().ToByteArray(),
            Topic: "topic",
            Broker: "rabbitmq",
            Headers: headers ?? new Dictionary<string, string?>(),
            MessageId: Guid.CreateVersion7(),
            SentAtUtc: new DateTimeOffset(2026, 10, 30, 12, 0, 0, TimeSpan.Zero));
}
