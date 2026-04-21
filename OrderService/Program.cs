using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OrderService;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<OrderConsumer>();

var host = builder.Build();
host.Run();