package com.kafka.producer;

import com.avro.generated.Employee;

public interface EventProducer
{
    void send(String key, Message msg);
    void send(String key, Employee msg);
}
