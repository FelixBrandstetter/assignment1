using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MoneyLaunderingService
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
                    collection.AddHostedService<MoneyLaunderingService>();
                });
    }

    public class MoneyLaunderingService : IHostedService
    {
        private readonly ILogger<MoneyLaunderingService> logger;
        private readonly IProducer<Null, string> _producer;

        public MoneyLaunderingService(ILogger<MoneyLaunderingService> logger)
        {
            this.logger = logger;
            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9092"
            };
            _producer = new ProducerBuilder<Null, string>(config).Build();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("Listening for messages from topic \"payments\"...");

            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "foo",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe("payments");

            while (true)
            {
                var consumeResult = consumer.Consume(cancellationToken);
                int valueResult = int.Parse(consumeResult.Message.Value);

                logger.LogInformation($"CONSUMER of payments: {valueResult}");

                bool declined = false;

                if (valueResult > 1000)
                {
                    declined = true;
                }

                logger.LogInformation($"Sending declined: {declined} to launderycheck");

                await _producer.ProduceAsync("launderycheck", new Message<Null, string>()
                {
                    Value = declined.ToString()
                }, cancellationToken);
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _producer?.Dispose();
            return Task.CompletedTask;
        }
    }
}
