package com.kafka.producer;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

public class KeySerializer implements Serializer<String>
{

    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {

    }

    @Override
    public byte[] serialize(String topic, String data)
    {
        return null;
    }

    @Override
    public void close()
    {

    }

}
