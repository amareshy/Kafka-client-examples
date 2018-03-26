package com.kafka.consumer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.kafka.producer.Message;

public class ValueDeserializer implements Deserializer<Message>
{

    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {

    }

    @Override
    public Message deserialize(String topic, byte[] data)
    {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInput in = null;
        Message msg = null;
        try
        {
            in = new ObjectInputStream(bis);
            msg = (Message) in.readObject();
        }
        catch (IOException | ClassNotFoundException e)
        {
            e.printStackTrace();
        }
        finally
        {
            try
            {
                if (in != null)
                {
                    in.close();
                }
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
            }
        }
        return msg;
    }

    @Override
    public void close()
    {

    }

}
