package com.kafka.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.kafka.producer.Message;

public class EventConsumer
{
    private ExecutorService executorService = null;
    private List<String> topics;
    private Properties props;
    private int noOfConsumers;

    public EventConsumer(List<String> topics, int noOfConsumers, Properties props)
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
        List<EventDataConsumer> consumers = new ArrayList<EventDataConsumer>(noOfConsumers);
        for (int i = 0; i < noOfConsumers; i++)
        {
            consumers.add(new EventDataConsumer());
        }
        executorService.invokeAll(consumers);
    }

    private class EventDataConsumer implements Callable<KafkaConsumer<String, Message>>
    {

        @Override
        public KafkaConsumer<String, Message> call()
        {
            KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(props);
            try
            {
                //Kafka Consumer subscribes list of topics here.
                consumer.subscribe(topics);
                System.out.println("Subscribed to topic " + topics);

                while (true)
                {
                    ConsumerRecords<String, Message> records = consumer.poll(100);
                    for (ConsumerRecord<String, Message> record : records)
                    {
                        // print the offset,key and value for the consumer records.
                        System.out.printf("Processed by Consumer : " + Thread.currentThread().getName() + " => offset = %d, key = %s, value = %s, topic = %s, partition = %s\n",
                                record.offset(), record.key(), record.value().getUserId() + " " + record.value().getMessage(), record.topic(), record.partition());
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
