package com.kafka.producer;

public class TopicPartitionSelector
{
    private final int maxPartitions = 10;
    private int currentPartition = 0;

    public int getNextPartition()
    {
        if (currentPartition == maxPartitions)
        {
            currentPartition = 0;
        }
        return currentPartition++ % maxPartitions;
    }

}
