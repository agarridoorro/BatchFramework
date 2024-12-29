package com.ango.batch;

public interface IJob
{
    public default String name()
    {
        return getClass().getName();
    }

    void execute();
}
