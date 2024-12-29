package com.ango.batch;

import java.util.List;

public interface IWriter<T>
{
    void write(List<T> items);
}
