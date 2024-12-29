package com.ango.batch.job;

import java.util.UUID;

import com.ango.batch.IJob;
import com.ango.batch.IStepStatus;
import com.ango.batch.exceptions.StepExecutionException;

public class JobExecutor
{
    private UUID id;
    private long initTime;
    private long endTime;
    private long elapsed;

    private IJob job;

    public JobExecutor()
    {
        id = UUID.randomUUID();
        initTime = 0;
        endTime = 0;
        elapsed = 0;
    }

    public void execute(IJob imp)
    {
        try
        {
            start();
            job.execute();
            stop();
        }
        catch (StepExecutionException e)
        {
            stop(e);
        }
        catch (Throwable e)
        {
            stop(e);
        }
    }

    private void start()
    {
        initTime = System.currentTimeMillis();
        //TODO: insert job
    }

    private void stop()
    {
        _stop();
        //TODO: update job
    }

    private void stop(StepExecutionException e)
    {
        _stop();
        final IStepStatus status = e.getStatus();
        for (Throwable t : status.exceptions())
        {
            t.printStackTrace();
        }
        //TODO: update job
    }

    private void stop(Throwable t)
    {
        _stop();
        t.printStackTrace();
        //TODO: update job
    }

    private void _stop()
    {
        endTime = System.currentTimeMillis();
        elapsed = endTime - initTime;
    }
}
