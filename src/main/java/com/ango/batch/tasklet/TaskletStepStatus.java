package com.ango.batch.tasklet;

import com.ango.batch.status.CommonStatus;

public class TaskletStepStatus extends CommonStatus
{
    private int committed = 0;

    public TaskletStepStatus(String name)
    {
        super(name);
    }

    @Override
    public void reset()
    {
        super.reset();
        committed = 0;
    }

    public void commit()
    {
        committed = 1;
    }

    public void undo()
    {
        committed = 0;
    }

    @Override
    public int read()
    {
        return 0;
    }

    @Override
    public int skipped()
    {
        return 0;
    }

    @Override
    public int written()
    {
        return 0;
    }

    @Override
    public long lastElapsed()
    {
        return (endTime() == 0) ? 0 : endTime() - initTime();
    }

    @Override
    public int committed()
    {
        return committed;
    }
}
