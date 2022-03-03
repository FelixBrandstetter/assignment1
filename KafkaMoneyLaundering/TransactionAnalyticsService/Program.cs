using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace TransactionAnalyticsService
{
    class Program
    {
        static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        private static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, collection) =>
                {
                    collection.AddHostedService<TransactionAnalyticsService>();
                });   
    }

    public class TransactionAnalyticsService : IHostedService
    {
        private readonly ILogger<TransactionAnalyticsService> logger;

        public TransactionAnalyticsService(ILogger<TransactionAnalyticsService> logger)
        {
            this.logger = logger;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("Listening for messages from topic \"launderycheck\"...");

            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "foo",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe("launderycheck");

            int declinedCount = 0;
            int acceptedCount = 0;

            while (true)
            {
                var consumeResult = consumer.Consume(cancellationToken);
                bool declinedResult = bool.Parse(consumeResult.Message.Value);

                if (declinedResult)
                {
                    declinedCount++;
                }
                else
                {
                    acceptedCount++;
                }

                logger.LogInformation($"CONSUMER of launderycheck: {declinedResult}");
                logger.LogInformation($"declinedCount: {declinedCount}");
                logger.LogInformation($"acceptedCount: {acceptedCount}");
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
