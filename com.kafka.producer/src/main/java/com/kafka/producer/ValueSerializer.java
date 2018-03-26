package com.kafka.producer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

public class ValueSerializer implements Serializer<Message>
{

    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {

    }

    @Override
    public byte[] serialize(String topic, Message data)
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] bytes = null;
        try (ObjectOutput out = new ObjectOutputStream(bos);)
        {
            out.writeObject(data);
            out.flush();
            bytes = bos.toByteArray();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            try
            {
                bos.close();
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
            }
        }
        return bytes;
    }

    @Override
    public void close()
    {

    }

}
