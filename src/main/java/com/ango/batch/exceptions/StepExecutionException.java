package com.ango.batch.exceptions;

import com.ango.batch.IStepStatus;

public class StepExecutionException extends RuntimeException
{
    private final IStepStatus status;

    public StepExecutionException(IStepStatus status)
    {
        super();
        this.status = status;
    }

    public IStepStatus getStatus()
    {
        return status;
    }
}
