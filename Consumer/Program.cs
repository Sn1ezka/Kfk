using System;
using System.Threading;
using Confluent.Kafka;

class Program
{
    public static void Main(string[] args)
    {
        var conf = new ConsumerConfig
        { 
            GroupId = "test-consumer-group",
            BootstrapServers = "localhost:29092",
            // Примечание: свойство AutoOffsetReset определяет начальное смещение в событии.
            // еще нет подтвержденных смещений для группы потребителей для
            // интересующие темы / разделы. По умолчанию смещения фиксируются
            // автоматически, поэтому в этом примере потребление начнется только с
            // самое раннее сообщение в теме 'test-topic' при первом запуске программы.
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
        {
            c.Subscribe("test-topic");

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // предотвратить завершение процесса.
                cts.Cancel();
            };

            try
            {
                while (true)
                {
                    try
                    {
                        var cr = c.Consume(cts.Token);
                        Console.WriteLine($"Consumed message '{cr.Message.Value}' at: '{cr.TopicPartitionOffset}'.");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Убедитесь, что потребитель чисто покидает группу и совершены окончательные взаимозачеты.
                c.Close();
            }
        }
    }
}