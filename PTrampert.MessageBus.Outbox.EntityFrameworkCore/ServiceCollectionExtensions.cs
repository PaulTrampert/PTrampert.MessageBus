using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace PTrampert.MessageBus.Outbox.EntityFrameworkCore;

/// <summary>
/// Extension methods for registering the Entity Framework Core outbox with the <see cref="IServiceCollection"/>.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the transactional outbox, stored in the application's <typeparamref name="TDbContext"/>.
    /// Messages published through <see cref="IOutboxPublisher"/> are saved by the application's own
    /// <see cref="DbContext.SaveChangesAsync(CancellationToken)"/>, and sent once that has committed.
    /// </summary>
    /// <typeparam name="TDbContext">
    /// The application's <see cref="DbContext"/>, registered as a scoped service (for example with
    /// <c>AddDbContext</c>). Map the outbox into its model with
    /// <see cref="ModelBuilderExtensions.AddPTrampertMessageBusOutbox"/>.
    /// </typeparam>
    /// <param name="services">The <see cref="IServiceCollection"/> to add services to.</param>
    /// <returns>The same <see cref="IServiceCollection"/> instance so calls can be chained.</returns>
    public static IServiceCollection AddPTrampertMessageBusEntityFrameworkOutbox<TDbContext>(this IServiceCollection services)
        where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);

        return services.AddPTrampertMessageBusOutbox<EntityFrameworkOutboxStore<TDbContext>>();
    }
}
