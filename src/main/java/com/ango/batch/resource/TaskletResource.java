package com.ango.batch.resource;

import com.ango.batch.ITasklet;

public class TaskletResource extends AbstractResource implements ITasklet
{
    private final ITasklet tasklet;

    public TaskletResource(ITasklet tasklet)
    {
        super(tasklet);
        this.tasklet = tasklet;
    }

    @Override
    public void execute()
    {
        tasklet.execute();
    }

}
