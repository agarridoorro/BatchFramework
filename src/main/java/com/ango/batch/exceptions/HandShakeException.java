package com.ango.batch.exceptions;

public class HandShakeException extends RuntimeException
{
    private final Throwable exception;

    public HandShakeException(Throwable t)
    {
        super();
        this.exception = t;
    }

    public Throwable getException()
    {
        return exception;
    }
}
