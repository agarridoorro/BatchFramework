package com.ango.batch.step;

import com.ango.batch.IStepStatus;
import com.ango.batch.exceptions.StepExecutionException;

import javax.transaction.TransactionManager;

public abstract class CommonStep<T extends IStepStatus>
{
    private T status;
    private TransactionManager tm;
    private boolean throwExceptions;

    protected T status() { return status; }

    public void setStatus(T status)
    {
        this.status = status;
    }

    protected TransactionManager transactionManager()
    {
        return tm;
    }

    public void setTransactionManager(TransactionManager tm)
    {
        this.tm = tm;
    }

    public void setThrowExceptions(boolean value)
    {
        this.throwExceptions = value;
    }

    protected T checkStatus()
    {
        if (status.state().isFailed() && throwExceptions)
        {
            throw new StepExecutionException(status);
        }
        return status;
    }
}
