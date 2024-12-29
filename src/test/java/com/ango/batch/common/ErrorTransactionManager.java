package com.ango.batch.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ErrorTransactionManager implements TransactionManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorTransactionManager.class);

    private final AtomicInteger numberOfBegins = new AtomicInteger(0);
    private final AtomicInteger numberOfCommits = new AtomicInteger(0);
    private final AtomicInteger numberOfRollbacks = new AtomicInteger(0);
    private int beginsToFail;
    private int commitsToFail;
    private int rollbacksToFail;
    private int waitDuration;
    private String waitThread;

    public ErrorTransactionManager()
    {
        this.beginsToFail = -1;
        this.commitsToFail = -1;
        this.rollbacksToFail = -1;
        this.waitDuration = -1;
        this.waitThread = "";
    }

    public ErrorTransactionManager setBeginsToFail(int beginsToFail)
    {
        this.beginsToFail = beginsToFail;
        return this;
    }

    public ErrorTransactionManager setCommitsToFail(int commitsToFail)
    {
        this.commitsToFail = commitsToFail;
        return this;
    }

    public ErrorTransactionManager setRollbacksToFail(int rollbacksToFail)
    {
        this.rollbacksToFail = rollbacksToFail;
        return this;
    }

    public ErrorTransactionManager setWaitDuration(int value)
    {
        this.waitDuration = value;
        return this;
    }

    public ErrorTransactionManager setWaitThread(String value)
    {
        this.waitThread = value.toLowerCase();
        return this;
    }

    @Override
    public void begin() throws NotSupportedException, SystemException
    {
        if (beginsToFail > 0)
        {
            if (waitThread.equalsIgnoreCase(Thread.currentThread().getName()) && waitDuration > 0)
            {
                try
                {
                    LOGGER.debug("waiting for begin");
                    Thread.sleep(waitDuration);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
            if (numberOfBegins.incrementAndGet() == beginsToFail)
            {
                throw new SystemException("Fail on begin transaction");
            }
            LOGGER.debug("begin");
        }
    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException
    {
        if (commitsToFail > 0)
        {
            if (waitThread.equalsIgnoreCase(Thread.currentThread().getName()) && waitDuration > 0)
            {
                try
                {
                    Thread.sleep(waitDuration);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
            if (numberOfCommits.incrementAndGet() == commitsToFail)
            {
                throw new SystemException("Fail on [" + numberOfCommits + "] commit");
            }
        }
        LOGGER.debug("commit");
    }

    @Override
    public int getStatus() throws SystemException
    {
        return 0;
    }

    @Override
    public Transaction getTransaction() throws SystemException
    {
        return null;
    }

    @Override
    public void resume(Transaction transaction) throws InvalidTransactionException, IllegalStateException, SystemException
    {

    }

    @Override
    public void rollback() throws IllegalStateException, SecurityException, SystemException
    {
        if (rollbacksToFail > 0)
        {
            if (waitThread.equalsIgnoreCase(Thread.currentThread().getName()) && waitDuration > 0)
            {
                try
                {
                    Thread.sleep(waitDuration);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
            if (numberOfRollbacks.incrementAndGet() == rollbacksToFail)
            {
                throw new SystemException("Fail on [" + numberOfRollbacks + "] rollback");
            }
        }
        LOGGER.debug("rollback");
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException
    {

    }

    @Override
    public void setTransactionTimeout(int i) throws SystemException
    {

    }

    @Override
    public Transaction suspend() throws SystemException
    {
        return null;
    }
}
