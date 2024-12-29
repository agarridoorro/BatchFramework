package com.ango.batch;

public interface IStepBuilder<T>
{
    T setName(String name);

    T setThrowExceptions(boolean value);

    IStep build();
}
