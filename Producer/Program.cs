using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Kafka
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:29092" };

            // Если сериализаторы не указаны, сериализаторы по умолчанию из
            // `Confluent.Producer.Serializers` будет автоматически использоваться там, где
            // доступный. Примечание: по умолчанию строки кодируются как UTF8.
            using (var p = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var dr = await p.ProduceAsync("test-topic", new Message<Null, string> { Value="test" });
                    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }
    }
}