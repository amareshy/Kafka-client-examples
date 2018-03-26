package com.kafka.producer;

public interface EventProducer
{
    void send(String key, Message msg);
}
