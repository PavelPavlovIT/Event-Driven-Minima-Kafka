using Common;
using Confluent.Kafka;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Data.SqlClient;
using System.Text.Json;

namespace OrderService;

public class OrderConsumer(
    ILogger<OrderConsumer> logger,
    IConfiguration configuration) : BackgroundService
{
    private readonly string _connectionString = configuration.GetConnectionString("DefaultConnection")
        ?? throw new InvalidOperationException("Connection string not found.");

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "order-processor-group", // Все инстансы в этой группе делят партиции
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false // Коммитим только когда база подтвердила запись
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe("orders.raw");

        logger.LogInformation("OrderService запущен и слушает топик orders.raw...");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // 1. Получаем сообщение из Кафки
                var result = consumer.Consume(stoppingToken);
                if (result == null) continue;
                if (string.IsNullOrWhiteSpace(result.Message.Value) || result.Message.Value == "{}")
                {
                    logger.LogWarning("Пропущено пустое сообщение на офсете {offset}", result.Offset);
                    consumer.Commit(result); // Подтверждаем, чтобы больше его не видеть
                    continue;
                }

                var orderId = result.Message.Key; // Тот самый GUID с фронта/шлюза
                var request = JsonSerializer.Deserialize<CreateOrderRequest>(result.Message.Value,    
                    new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                logger.LogInformation($"Начата обработка заказа {orderId} из партиции {result.Partition}");

                // 2. Атомарная транзакция в БД
                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync(stoppingToken);
                using var transaction = connection.BeginTransaction();

                try
                {
                    // Вставляем заказ (бизнес-логика)
                    const string insertOrderSql = @"
                        IF NOT EXISTS (SELECT 1 FROM Orders WHERE Id = @Id)
                        INSERT INTO Orders (Id, Amount, CustomerName, Status) 
                        VALUES (@Id, @Amount, @Name, 'Processing')";

                    await connection.ExecuteAsync(insertOrderSql,
                        new { 
                            Id = orderId,
                            Amount = request?.Amount ?? 0, // Это уже decimal!
                            Name = request?.CustomerName ?? "Unknown"
                        },
                        transaction);

                    // Вставляем событие в Outbox (транспорт для Debezium)
                    const string insertOutboxSql = @"
                        INSERT INTO Outbox (AggregateId, Type, Payload) 
                        VALUES (@Id, 'OrderAccepted', @Payload)";

                    await connection.ExecuteAsync(insertOutboxSql,
                        new { Id = orderId, Payload = result.Message.Value },
                        transaction);

                    // Фиксируем всё в БД
                    await transaction.CommitAsync(stoppingToken);

                    // 3. Подтверждаем офсет в Кафке
                    consumer.Commit(result);

                    logger.LogInformation($"Заказ {orderId} успешно сохранен и передан в Outbox.");
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    logger.LogError(ex, $"Ошибка транзакции для заказа {orderId}. Откатываемся.");
                    // Тут мы НЕ делаем consumer.Commit(), поэтому сообщение придет снова
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                logger.LogError(ex, "Критическая ошибка в цикле обработки.");
                await Task.Delay(2000, stoppingToken); // Пауза перед ретраем
            }
        }
    }
}