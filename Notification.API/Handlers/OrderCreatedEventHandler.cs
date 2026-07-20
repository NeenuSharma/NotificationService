using Shared.Events.Events;

namespace Notification.API.Handlers;

public class OrderCreatedEventHandler
    : IOrderCreatedEventHandler
{
    public Task HandleAsync(OrderCreatedEvent orderEvent)
    {
        Console.WriteLine();
        Console.WriteLine("===== Processing Order =====");

        Console.WriteLine($"Order Id     : {orderEvent.OrderId}");
        Console.WriteLine($"Product      : {orderEvent.ProductName}");
        Console.WriteLine($"Quantity     : {orderEvent.Quantity}");
        Console.WriteLine($"Amount       : {orderEvent.TotalAmount}");

        Console.WriteLine("============================");
        Console.WriteLine();

        // Later we'll:
        // Send Email
        // Save Notification
        // Send SMS

        return Task.CompletedTask;
    }
}