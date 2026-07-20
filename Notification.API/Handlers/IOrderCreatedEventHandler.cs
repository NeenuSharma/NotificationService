using Shared.Events.Events;

namespace Notification.API.Handlers
{
    public interface IOrderCreatedEventHandler
    {
        Task HandleAsync(OrderCreatedEvent orderEvent);
    }
}
