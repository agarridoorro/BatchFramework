package com.ango.batch.tasklet;

import com.ango.batch.ITasklet;

public class TestTasklet implements ITasklet
{
    @Override
    public void execute()
    {
        System.out.println("Executing...");
    }
}
