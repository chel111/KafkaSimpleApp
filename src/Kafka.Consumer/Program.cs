using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Consumer
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
        static void Main(string[] args)
        {
            if (!args.Any())
            {
                Console.WriteLine("Enter topic name");
                return;
            }
            //Kafka consumer configuration settings
            var topicName = args.First();

            var config = new ConsumerConfig
            {
                BootstrapServers = "kafka:9092",
                Acks = Acks.All,
                GroupId = "test",
                AutoCommitIntervalMs = 5000,
                EnableAutoCommit = true,
                SessionTimeoutMs = 100000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };

            var randomSampling = new List<Order>();
            var k = 10;
            var reservoirSampling = new List<Order>(Enumerable.Repeat<Order>(null, k));

            using var consumer = new ConsumerBuilder<string, string>(config).Build();

            //Kafka Consumer subscribes list of topics here.
            consumer.Subscribe(topicName);
            Console.WriteLine("Subscribed to topic " + topicName);

            int i = 0;
            while (true)
            {
                if (i == 299)
                {
                    break;
                }
                var result = consumer.Consume();

                if (result?.Message?.Value == null)
                    continue;

                var order = JsonConvert.DeserializeObject<Order>(result.Message.Value);
                var x = new Random().Next(0, i + 1);
                if (x < k)
                {
                    reservoirSampling[x] = order;
                }

                if (Math.Abs(order.OrderId.GetHashCode()) % 10 != 0)
                {
                    Console.WriteLine($"Skipping message");
                    i++;
                    continue;
                }

                randomSampling.Add(order);
                Console.WriteLine(
                    new string('-', 20) +
                    $"\nOrder: Id = {order.OrderId}, Sum = {order.Sum}, Time = {order.Time}\n" +
                    new string('-', 20)); 
                i++;
            }
            Console.WriteLine($"\nTotal number in the random sampling = {randomSampling.Count}");
            foreach (var order in reservoirSampling)
            {
                Console.WriteLine($"Order: Id = {order.OrderId}, Sum = {order.Sum}, Time = {order.Time}");
            }
        }
    }
}
