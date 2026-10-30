using System.Threading.Channels;

namespace PTrampert.MessageBus.Outbox;

internal sealed class OutboxNotifier : IOutboxNotifier
{
    // Holds at most one pending signal, so a notification that arrives while the dispatcher is busy is not lost.
    private readonly Channel<bool> _signal = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });

    public void Notify()
    {
        _signal.Writer.TryWrite(true);
    }

    /// <summary>
    /// Waits until <see cref="Notify"/> is called or <paramref name="timeout"/> elapses, whichever comes first.
    /// </summary>
    public async Task WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(timeout);
        try
        {
            await _signal.Reader.WaitToReadAsync(timeoutSource.Token);
            _signal.Reader.TryRead(out _);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
        }
    }
}
