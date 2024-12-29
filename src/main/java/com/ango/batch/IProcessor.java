package com.ango.batch;

public interface IProcessor<T,K>
{
    K process(T item);
}
