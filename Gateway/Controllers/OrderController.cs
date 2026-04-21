using Common;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace Gateway.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class OrdersController : ControllerBase
    {
        private readonly IProducer<string, string> _producer;

        public OrdersController(IProducer<string, string> producer)
        {
            _producer = producer;
        }

        [HttpPost]
        public async Task<IActionResult> CreateOrder([FromBody] CreateOrderRequest request)
        {
            var message = new Message<string, string>
            {
                Key = Guid.NewGuid().ToString(),
                Value = JsonSerializer.Serialize(request)
            };

            // Отправляем в "сырой" топик
            await _producer.ProduceAsync("orders.raw", message);

            return Accepted(new { OrderId = message.Key }); // 202 Accepted - классика для асинхронщины
        }
    }
}
