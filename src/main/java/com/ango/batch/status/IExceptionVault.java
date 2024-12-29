package com.ango.batch.status;

import java.util.List;

public interface IExceptionVault
{
    void add(Throwable t);

     void addAll(List<Throwable> errors);

    List<Throwable> exceptions();
}
