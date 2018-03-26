package com.kafka.producer;

import java.util.Date;

public class EventGenerator
{

    /**
     * @param args
     */
    public static void main(String[] args)
    {
        EventProducer producer = new KafkaProducerImpl();
        for (int i = 0; i < 100; i++)
        {
            Message msg = new Message();
            msg.setUserId(Integer.toString(i));
            msg.setMessage("User" + i + " Modified @ " + new Date(System.currentTimeMillis()));
            producer.send(Integer.toString(i), msg);
        }
    }

}
