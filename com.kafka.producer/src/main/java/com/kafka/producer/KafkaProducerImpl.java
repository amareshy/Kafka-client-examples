package com.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.avro.generated.Employee;

public class KafkaProducerImpl implements EventProducer
{

    private final BlockingQueue<ProducerRecord<String, Message>> queue = new ArrayBlockingQueue<ProducerRecord<String, Message>>(1024);
    private final KafkaProducer<String, Message> myProducer;
    private final String topic = "test-event-topic";
    private final TopicPartitionSelector topicPartitionSelector;

    ExecutorService myExecutorService = Executors.newSingleThreadExecutor(new ThreadFactory()
    {
        int count = 0;

        @Override
        public Thread newThread(Runnable r)
        {
            return new Thread(r, "Kafka-Producer-Thread-" + (count++));
        }
    });

    public KafkaProducerImpl()
    {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ValueSerializer.class.getName());

        myProducer = new KafkaProducer<>(props);
        myExecutorService.execute(new MessageDataProducer());
        topicPartitionSelector = new TopicPartitionSelector();
    }

    @Override
    public void send(String key, Message msg)
    {

        ProducerRecord<String, Message> record = new ProducerRecord<String, Message>(topic, topicPartitionSelector.getNextPartition(), key, msg);
        queue.add(record);
    }

    private class MessageDataProducer implements Runnable
    {
        @Override
        public void run()
        {
            while (true)
            {
                try
                {
                    ProducerRecord<String, Message> record = queue.take();
                    myProducer.send(record, new Callback()
                    {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception e)
                        {
                            if (e != null)
                            {
                                e.printStackTrace();
                            }
                            else
                            {
                                System.out.println("The offset of the record we just sent is: " + metadata.offset() + " topic : " + metadata.topic() + " partition : " + metadata.partition());
                            }

                        }
                    });

                    TimeUnit.MILLISECONDS.sleep(500);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        }

    }

	@Override
	public void send(String key, Employee msg) {
		// TODO Not getting used
		
	}
}
