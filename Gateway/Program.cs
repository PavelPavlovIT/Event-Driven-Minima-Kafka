using Confluent.Kafka;
using Microsoft.AspNetCore.Builder;
using Scalar.AspNetCore;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

var redisConnection = builder.Configuration.GetConnectionString("Redis") ?? "localhost:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(ConnectionMultiplexer.Connect(redisConnection));

// 1. Встроенная поддержка OpenAPI в .NET 9
builder.Services.AddOpenApi();

builder.Services.AddControllers();

// 2. Правильная регистрация Kafka Producer
var producerConfig = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    Acks = Acks.Leader
};

builder.Services.AddSingleton<IProducer<string, string>>(sp =>
    new ProducerBuilder<string, string>(producerConfig).Build());

var app = builder.Build();

// 3. Маппинг OpenAPI эндпоинта
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi(); // Генерирует JSON описание по адресу /openapi/v1.json
    app.MapScalarApiReference();
}

app.UseHttpsRedirection();
app.MapControllers();

app.Run();