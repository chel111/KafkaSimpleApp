using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Producer
{
    public class Order
    {
        [JsonRequired]
        [JsonProperty("OrderId")]
        public string OrderId { get; set; }

        [JsonRequired]
        [JsonProperty("Sum")]
        public double Sum { get; set; }

        [JsonRequired]
        [JsonProperty("Time")]
        public DateTime Time { get; set; }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            if (!args.Any())
            {
                Console.WriteLine("Enter topic name");
                return;
            }

            var topicName = args.First();

            var config = new ProducerConfig
            {
                BootstrapServers = "kafka:9092",
                Acks = Acks.All,
                MessageSendMaxRetries = 0,
                BatchSize = 16384,
                LingerMs = 1,
                QueueBufferingMaxKbytes = 33554432
            };

            using var producer = new ProducerBuilder<string, string>(config).Build();

            for (var i = 0; i < 300; i++)
            {
                var now = DateTime.UtcNow;

                await producer.ProduceAsync(topicName, new Message<string, string>
                {
                    Key = now.ToString(),
                    Value = JsonConvert.SerializeObject(new Order
                    {
                        OrderId = Faker.Identification.MedicareBeneficiaryIdentifier(),
                        Sum = Faker.RandomNumber.Next(100, 500),
                        Time = now
                    })
                });

                Console.WriteLine("Message sent successfully");
            }
        }
    }
}
