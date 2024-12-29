package com.ango.batch.exceptions;

import com.ango.batch.chunk.multi.ConsumerPhase;

public class ConsumerException extends RuntimeException
{
    private final int index;
    private final ConsumerPhase phase;
    private final Throwable exception;

    public ConsumerException(int index, ConsumerPhase phase, Throwable t)
    {
        super();
        this.index = index;
        this.phase = phase;
        this.exception = t;
    }

    public int getIndex()
    {
        return index;
    }

    public ConsumerPhase getPhase()
    {
        return phase;
    }

    public Throwable getException()
    {
        return exception;
    }
}
