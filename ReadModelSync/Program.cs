using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using ReadModelSync;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<RedisSyncWorker>();

var host = builder.Build();
host.Run();