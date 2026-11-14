using PTrampert.MessageBus.Outbox.EntityFrameworkCore;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace PTrampert.MessageBus.Test.Outbox;

/// <summary>An application <see cref="DbContext"/> with the outbox mapped into it, next to the application's own data.</summary>
public sealed class OutboxTestDbContext(DbContextOptions<OutboxTestDbContext> options) : DbContext(options)
{
    public DbSet<Widget> Widgets => Set<Widget>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.AddPTrampertMessageBusOutbox();
    }

    /// <summary>
    /// A connection string for a new SQLite database file. A file, rather than an in-memory database, lets every
    /// context open its own connection, as the contexts of separate scopes would against a real database.
    /// </summary>
    public static string CreateConnectionString() =>
        new SqliteConnectionStringBuilder
        {
            DataSource = Path.Combine(Path.GetTempPath(), $"ptrampert-messagebus-{Guid.NewGuid():N}.db"),
            Pooling = false,
        }.ToString();

    public static void DeleteDatabase(string connectionString)
    {
        File.Delete(new SqliteConnectionStringBuilder(connectionString).DataSource);
    }
}

public sealed class Widget
{
    public Guid Id { get; set; }
    public string Name { get; set; } = null!;
}
