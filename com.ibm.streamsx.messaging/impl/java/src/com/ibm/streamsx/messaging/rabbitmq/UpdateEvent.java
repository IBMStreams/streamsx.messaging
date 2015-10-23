package com.ibm.streamsx.messaging.rabbitmq;


import java.io.IOException;


public interface UpdateEvent
{
    // This is just a regular method so it can return something or
    // take arguments if you like.
    public void NotifyUpdateEvent (String guid, String message);
}
