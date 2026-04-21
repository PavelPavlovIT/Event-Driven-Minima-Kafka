using Common;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using StackExchange.Redis;
using System.Text.Json;

namespace Gateway.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class OrdersController : ControllerBase
    {
        private readonly IProducer<string, string> _producer;
        private readonly IConnectionMultiplexer _redisConnection;
        public OrdersController(IProducer<string, string> producer, IConnectionMultiplexer redisConnection)
        {
            _producer = producer;
            _redisConnection = redisConnection;
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

        [HttpGet("{id}")]
        public async Task<IActionResult> GetOrder(string id)
        {
            var redis = _redisConnection.GetDatabase();
            var orderData = await redis.StringGetAsync($"order:{id}");

            if (!orderData.HasValue)
            {
                return NotFound(new { Message = "Заказ еще не обработан или не существует" });
            }

            // Возвращаем чистый JSON из редиса
            return Content(orderData, "application/json");
        }
    }
}
