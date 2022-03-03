using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace CoreBankingSystemMock
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
                    collection.AddHostedService<BankingMockService>();
                });
    }

    public class BankingMockService : IHostedService
    {
        private readonly ILogger<BankingMockService> logger;
        private readonly IProducer<Null, string> _producer;
        private readonly Random random;

        public BankingMockService(ILogger<BankingMockService> logger)
        {
            this.logger = logger;
            this.random = new Random();

            var config = new ProducerConfig()
            {
                BootstrapServers = "localhost:9092"
            };

            _producer = new ProducerBuilder<Null, string>(config).Build();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("Press any button to send mock data");

            while (true)
            {
                Console.ReadKey();

                int randomAmount = this.random.Next(0, 2000);
                logger.LogInformation(randomAmount.ToString());
                await _producer.ProduceAsync("payments", new Message<Null, string>()
                {
                    Value = randomAmount.ToString()
                }, cancellationToken);
            };
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _producer?.Dispose();
            return Task.CompletedTask;
        }
    }
}
