package com.kafka.producer.avro;

import com.avro.generated.Employee;
import com.kafka.producer.EventProducer;

public class AvroEventGenerator
{

    /**
     * @param args
     */
    public static void main(String[] args)
    {
        EventProducer producer = new AvroDataKafkaProducerImpl();
        for (int i = 0; i < 100; i++)
        {
        	Employee emp = Employee.newBuilder().setName("Amaresh").setId(i).setAge(32).setAddress("Faridabad")
    				.setSalary(1000000 + i).build();
            producer.send(Integer.toString(i), emp);
        }
    }

}
