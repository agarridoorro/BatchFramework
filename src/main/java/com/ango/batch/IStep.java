package com.ango.batch;

public interface IStep
{
    String name();

    IStepStatus execute();
}
