package com.ango.batch.tasklet;

import com.ango.batch.ITasklet;
import com.ango.batch.common.ErrorResource;

public class DoNothingTasklet extends ErrorResource implements ITasklet
{
    private  boolean fail = false;

    public DoNothingTasklet(boolean fail, boolean failOnOpen, boolean failOnClose)
    {
        super(failOnOpen, failOnClose);
        this.fail = fail;
    }

    @Override
    public void execute()
    {
        try
        {
            Thread.sleep(1);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        if (fail)
        {
            throw new RuntimeException("Error!!!");
        }
    }
}
