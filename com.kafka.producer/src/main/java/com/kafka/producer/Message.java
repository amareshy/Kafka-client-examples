package com.kafka.producer;

import java.io.Serializable;

public class Message implements Serializable
{
    private static final long serialVersionUID = -1337582197242959635L;

    private String userId;
    private String message;

    /**
     * @return the userId
     */
    public String getUserId()
    {
        return userId;
    }

    /**
     * @return the message
     */
    public String getMessage()
    {
        return message;
    }

    /**
     * @param parUserId the userId to set
     */
    public void setUserId(String parUserId)
    {
        userId = parUserId;
    }

    /**
     * @param parMessage the message to set
     */
    public void setMessage(String parMessage)
    {
        message = parMessage;
    }
}
