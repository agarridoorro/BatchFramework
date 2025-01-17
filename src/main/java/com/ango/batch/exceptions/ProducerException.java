package com.ango.batch.exceptions;

import com.ango.batch.chunk.multi.ConsumerPhase;

public class ProducerException extends RuntimeException
{
    private final Throwable exception;

    public ProducerException(Throwable t)
    {
        super();
        this.exception = t;
    }

    public Throwable getException()
    {
        return exception;
    }
}
