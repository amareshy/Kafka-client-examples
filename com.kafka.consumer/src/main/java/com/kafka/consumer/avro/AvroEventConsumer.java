package com.kafka.consumer.avro;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class AvroEventConsumer
{
    private ExecutorService executorService = null;
    private List<String> topics;
    private Properties props;
    private int noOfConsumers;

    public AvroEventConsumer(List<String> topics, int noOfConsumers, Properties props)
    {
        this.topics = topics;
        this.props = props;
        this.noOfConsumers = noOfConsumers;

        executorService = Executors.newFixedThreadPool(noOfConsumers, new ThreadFactory()
        {
            int count = 0;

            @Override
            public Thread newThread(Runnable r)
            {
                return new Thread(r, "EVENT-CONSUMER-THREAD-" + count++);
            }
        });
    }

    public void createAndStartConsumers() throws InterruptedException
    {
        //Create consumers
        List<AvroEventDataConsumer> consumers = new ArrayList<AvroEventDataConsumer>(noOfConsumers);
        for (int i = 0; i < noOfConsumers; i++)
        {
            consumers.add(new AvroEventDataConsumer());
        }
        executorService.invokeAll(consumers);
    }

    private class AvroEventDataConsumer implements Callable<KafkaConsumer<String, GenericRecord>>
    {

        @Override
        public KafkaConsumer<String, GenericRecord> call()
        {
            KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
            try
            {
                //Kafka Consumer subscribes list of topics here.
                consumer.subscribe(topics);
                System.out.println("Subscribed to topic " + topics);

                while (true)
                {
                    ConsumerRecords<String, GenericRecord> records = consumer.poll(100);
                    for (ConsumerRecord<String, GenericRecord> record : records)
                    {
                        // print the offset,key and value for the consumer records.
                        System.out.printf("Processed by Consumer : " + Thread.currentThread().getName() + " => offset = %d, key = %s, value = %s, topic = %s, partition = %s\n",
                                record.offset(), record.key(), record.value().get("name") + " " + record.value().get("id") + " " + record.value().get("age") + " "+record.value().get("address") + " " + record.value().get("salary"), record.topic(), record.partition());
                    }
                    consumer.commitAsync();
                }
            }
            finally
            {
                consumer.close();
                System.out.println("DONE");
            }
        }

    }

}
