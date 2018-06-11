package com.kafka.consumer.avro;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

public class AvroEventConsumerCreator
{

    public static void main(String[] args)
    {
        List<String> topics = new ArrayList<String>();
        topics.add("event-topic");
        topics.add("Multibrokerapplication");
        topics.add("Hello-Kafka");
        topics.add("test-event-topic");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test2");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroValueDeserializer.class.getName());

        AvroEventConsumer eventConsumer = new AvroEventConsumer(topics, 3, props);
        try
        {
            eventConsumer.createAndStartConsumers();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

}
