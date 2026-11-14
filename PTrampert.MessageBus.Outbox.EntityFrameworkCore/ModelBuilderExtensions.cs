using Microsoft.EntityFrameworkCore;

namespace PTrampert.MessageBus.Outbox.EntityFrameworkCore;

/// <summary>
/// Extension methods for mapping the PTrampert.MessageBus outbox into an application's <see cref="DbContext"/>.
/// </summary>
public static class ModelBuilderExtensions
{
    /// <summary>
    /// The table the outbox is mapped to when no other name is given.
    /// </summary>
    public const string DefaultTableName = "MessageBusOutbox";

    /// <summary>
    /// Maps the outbox table into the model. Call this from <see cref="DbContext.OnModelCreating"/> of the
    /// <see cref="DbContext"/> passed to
    /// <see cref="ServiceCollectionExtensions.AddPTrampertMessageBusEntityFrameworkOutbox{TDbContext}"/>, then add
    /// a migration to create the table.
    /// </summary>
    /// <param name="modelBuilder">The model builder of the application's <see cref="DbContext"/>.</param>
    /// <param name="tableName">The name of the outbox table.</param>
    /// <param name="schema">The schema of the outbox table, or <see langword="null"/> for the default schema.</param>
    /// <returns>The same <see cref="ModelBuilder"/> instance so calls can be chained.</returns>
    public static ModelBuilder AddPTrampertMessageBusOutbox(
        this ModelBuilder modelBuilder,
        string tableName = DefaultTableName,
        string? schema = null)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        modelBuilder.Entity<OutboxMessage>(entity =>
        {
            entity.ToTable(tableName, schema);
            entity.HasKey(m => m.Sequence);
            entity.Property(m => m.Sequence).ValueGeneratedOnAdd();
            entity.HasIndex(m => m.MessageId).IsUnique();
            entity.Property(m => m.Topic).IsRequired();
            entity.Property(m => m.Broker).IsRequired();
            entity.Property(m => m.Body).IsRequired();
            entity.Property(m => m.Headers).IsRequired();
        });

        return modelBuilder;
    }
}
