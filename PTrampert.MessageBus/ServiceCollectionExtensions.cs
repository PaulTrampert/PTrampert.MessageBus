using System.Reflection;
using PTrampert.MessageBus.Registries;
using PTrampert.MessageBus.Helpers;
using PTrampert.MessageBus.Outbox;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace PTrampert.MessageBus;

/// <summary>
/// Extension methods for registering PTrampert.MessageBus services with the <see cref="IServiceCollection"/>.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers all core PTrampert.MessageBus services and applies the supplied configuration.
    /// Call this method (or a transport-specific overload that delegates to it) once during application startup.
    /// Any <see cref="IHandler{TMessage}"/> implementations already registered in the <see cref="IServiceCollection"/>
    /// are automatically subscribed — no explicit <see cref="BusConfigurator.RegisterHandler{THandler,TMessage}"/> call is needed.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> to add services to.</param>
    /// <param name="configure">
    /// An optional delegate for additional configuration (e.g. handlers with non-default routes or explicit broker targeting).
    /// When <see langword="null"/>, only handlers discovered from the service collection are registered.
    /// </param>
    /// <returns>The same <see cref="IServiceCollection"/> instance so calls can be chained.</returns>
    public static IServiceCollection AddPTrampertMessageBus(this IServiceCollection services, Action<BusConfigurator>? configure = null)
    {
        services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();
        services.AddSingleton<HandlerRegistry>();
        services.AddSingleton<TransportRegistry>();
        services.AddSingleton<MessageRouteResolver>();
        services.AddSingleton<IHostedService, BusService>();
        services.AddSingleton<BusConfigurator>(sp =>
        {
            var configurator = new BusConfigurator(
                sp.GetRequiredService<HandlerRegistry>(),
                sp.GetRequiredService<TransportRegistry>(),
                sp);
            configurator.RegisterTransports();
            configure?.Invoke(configurator);

            // Auto-subscribe every IHandler<> implementation that has been registered in the DI container.
            // Handlers already registered via the configure callback are skipped to avoid duplicate subscriptions.
            var discoveredHandlerTypes = services
                .Select(GetConcreteImplementationType)
                .OfType<Type>()
                .Where(t => t.GetInterfaces()
                    .Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IHandler<>)))
                .Distinct()
                .ToList();

            foreach (var handlerType in discoveredHandlerTypes)
            {
                if (!configurator.HandlerRegistry.IsHandlerRegistered(handlerType))
                {
                    configurator.HandlerRegistry.RegisterHandler(handlerType);
                }
            }

            return configurator;
        });
        services.AddScoped<IPublisher, DefaultPublisher>();
        
        return services;
    }

    /// <summary>
    /// Registers the transactional outbox: an <see cref="IOutboxPublisher"/> that stores messages through
    /// <typeparamref name="TOutboxStore"/>, and a hosted service that sends the stored messages once they are
    /// committed. Use together with <see cref="AddPTrampertMessageBus"/> (or a transport-specific overload), and
    /// configure the dispatcher through <see cref="BusConfigurator.OutboxPollingInterval"/> and
    /// <see cref="BusConfigurator.OutboxBatchSize"/>.
    /// </summary>
    /// <typeparam name="TOutboxStore">The <see cref="IOutboxStore"/> implementation, registered as a scoped service.</typeparam>
    /// <param name="services">The <see cref="IServiceCollection"/> to add services to.</param>
    /// <returns>The same <see cref="IServiceCollection"/> instance so calls can be chained.</returns>
    public static IServiceCollection AddPTrampertMessageBusOutbox<TOutboxStore>(this IServiceCollection services)
        where TOutboxStore : class, IOutboxStore
    {
        services.TryAddScoped<IOutboxStore, TOutboxStore>();
        services.TryAddScoped<IOutboxPublisher, OutboxPublisher>();
        services.TryAddSingleton<OutboxNotifier>();
        services.TryAddSingleton<IOutboxNotifier>(sp => sp.GetRequiredService<OutboxNotifier>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, OutboxDispatcher>());

        return services;
    }

    /// <summary>
    /// Returns the concrete implementation type for a <see cref="ServiceDescriptor"/>, or
    /// <see langword="null"/> when the type cannot be statically determined (e.g. factory or instance registrations
    /// where the service type is an interface or abstract class).
    /// </summary>
    private static Type? GetConcreteImplementationType(ServiceDescriptor descriptor)
    {
        if (descriptor.ImplementationType != null)
            return descriptor.ImplementationType;

        // Factory- and instance-based registrations don't expose a static implementation type,
        // but when the service is registered directly by its concrete class (e.g. services.AddScoped<MyHandler>()),
        // the ServiceType *is* the concrete type.
        if (descriptor.ImplementationFactory == null
            && descriptor.ImplementationInstance == null
            && !descriptor.ServiceType.IsInterface
            && !descriptor.ServiceType.IsAbstract)
        {
            return descriptor.ServiceType;
        }

        return null;
    }

    /// <summary>
    /// Scans the given assemblies for <see cref="IHandler{TMessage}"/> implementations and registers each as a
    /// scoped service using its concrete type. When used together with <see cref="AddPTrampertMessageBus"/>, the discovered
    /// handlers are automatically subscribed — no explicit
    /// <see cref="BusConfigurator.RegisterHandler{THandler,TMessage}"/> call is needed.
    /// If no assemblies are provided, all assemblies currently loaded in the application domain are scanned.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> to add services to.</param>
    /// <param name="assemblies">The assemblies to scan. When empty, all loaded assemblies are used.</param>
    /// <returns>The same <see cref="IServiceCollection"/> instance so calls can be chained.</returns>
    public static IServiceCollection AddPTrampertMessageBusHandlers(this IServiceCollection services, params Assembly[] assemblies)
    {
        assemblies = assemblies.Length > 0 ? assemblies : AppDomain.CurrentDomain.GetAssemblies();
        var handlerTypes = assemblies.SelectMany(
            assembly => assembly.GetLoadableTypes()
                .Where(type => type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IHandler<>)))
        );

        foreach (var handlerType in handlerTypes)
        {
            services.AddScoped(handlerType);
        }
        
        return services;
    }
}