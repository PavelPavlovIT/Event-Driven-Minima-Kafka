using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System.Text.Json;

namespace ReadModelSync;

public class RedisSyncWorker : BackgroundService
{
    private readonly ILogger<RedisSyncWorker> _logger;
    private readonly IConfiguration _configuration;
    private readonly IDatabase _redis;
    private readonly string _topicName;

    public RedisSyncWorker(ILogger<RedisSyncWorker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;

        // Подключаемся к Redis
        var redisConnection = _configuration.GetConnectionString("Redis") ?? "localhost:6379";
        var redis = ConnectionMultiplexer.Connect(redisConnection);
        _redis = redis.GetDatabase();

        // Название топика, который создает Debezium для таблицы Outbox
        // Обычно формат: [server].[database].[table]
        _topicName = "pavel_server.MyTestDB.dbo.Outbox";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "redis-sync-group-v1", // Уникальная группа для синхронизатора
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true // Для синхронизации кэша авто-коммит допустим
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(_topicName);

        _logger.LogInformation($"RedisSyncWorker запущен. Слушаю топик: {_topicName}");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                if (result?.Message?.Value == null) continue;

                // 1. Десериализуем конверт Debezium
                using var doc = JsonDocument.Parse(result.Message.Value);
                var payload = doc.RootElement;

                // Проверяем, что это операция вставки (create) или обновления (update)
                var op = payload.GetProperty("op").GetString();
                if (op == "c" || op == "u")
                {
                    var after = payload.GetProperty("after");

                    // Извлекаем данные из таблицы Outbox
                    var aggregateId = after.GetProperty("AggregateId").GetString();
                    var type = after.GetProperty("Type").GetString();
                    var dataJson = after.GetProperty("Payload").GetString();

                    if (!string.IsNullOrEmpty(aggregateId) && !string.IsNullOrEmpty(dataJson))
                    {
                        // 2. Формируем ключ и сохраняем в Redis
                        // Используем префикс для порядка в базе
                        string redisKey = $"order:{aggregateId}";

                        await _redis.StringSetAsync(redisKey, dataJson, TimeSpan.FromHours(24));

                        _logger.LogInformation($"[Redis] Успешно синхронизировано: {redisKey} (Тип: {type})");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Ошибка при синхронизации данных в Redis");
                await Task.Delay(5000, stoppingToken); // Ждем перед следующей попыткой
            }
        }

        consumer.Close();
    }
}