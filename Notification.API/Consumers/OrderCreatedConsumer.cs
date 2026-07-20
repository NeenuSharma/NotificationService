using Notification.API.Handlers;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared.Events.Events;
using System.Reflection.Metadata;
using System.Text;
using System.Text.Json;

namespace Notification.API.Consumers;

public class OrderCreatedConsumer : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly IOrderCreatedEventHandler _handler;

    public OrderCreatedConsumer(
      IConfiguration configuration,
      IOrderCreatedEventHandler handler)
    {
        _configuration = configuration;
        _handler = handler;
    }
    public OrderCreatedConsumer(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        // Create RabbitMQ Connection
        var factory = new ConnectionFactory
        {
            HostName = _configuration["RabbitMQ:HostName"],
            UserName = _configuration["RabbitMQ:UserName"],
            Password = _configuration["RabbitMQ:Password"]
        };

        await using var connection =
            await factory.CreateConnectionAsync();

        await using var channel =
            await connection.CreateChannelAsync();

        var exchange = _configuration["RabbitMQ:Exchange"];
        var queue = _configuration["RabbitMQ:Queue"];
        var routingKey = _configuration["RabbitMQ:RoutingKey"];

        // Declare Exchange
        await channel.ExchangeDeclareAsync(
            exchange: exchange,
            type: ExchangeType.Direct,
            durable: true);

        // Declare Queue
        await channel.QueueDeclareAsync(
            queue: queue,
            durable: true,
            exclusive: false,
            autoDelete: false);

        // Bind Queue
        await channel.QueueBindAsync(
            queue: queue,
            exchange: exchange,
            routingKey: routingKey);

        Console.WriteLine("=================================");
        Console.WriteLine("RabbitMQ Consumer Started...");
        Console.WriteLine("Waiting for messages...");
        Console.WriteLine("=================================");

        // Create Consumer
        var consumer = new AsyncEventingBasicConsumer(channel);

        // Event fired whenever a message arrives
        consumer.ReceivedAsync += async (sender, eventArgs) =>
        {
            try
            {
                // Read message body
                var body = eventArgs.Body.ToArray();

                // Convert bytes to JSON string
                var json = Encoding.UTF8.GetString(body);

                Console.WriteLine();
                Console.WriteLine("Message Received");
                Console.WriteLine(json);

                // Convert JSON to C# object
                var order =
                    JsonSerializer.Deserialize<OrderCreatedEvent>(json);

                if (order != null)
                {
                    await _handler.HandleAsync(order);
                }

                // Acknowledge message
                await channel.BasicAckAsync(
                    deliveryTag: eventArgs.DeliveryTag,
                    multiple: false);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Consumer Error");
                Console.WriteLine(ex.Message);

                // Reject message
                await channel.BasicNackAsync(
                    deliveryTag: eventArgs.DeliveryTag,
                    multiple: false,
                    requeue: true);
            }
        };

        // Start consuming messages
        await channel.BasicConsumeAsync(
            queue: queue,
            autoAck: false,
            consumer: consumer);

        // Keep service alive
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000, stoppingToken);
        }
    }
}