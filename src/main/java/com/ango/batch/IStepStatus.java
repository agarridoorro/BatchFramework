package com.ango.batch;

import java.util.List;

public interface IStepStatus
{
    String name();

    int read();

    int skipped();

    int written();

    int committed();

    long initTime();

    long endTime();

    long lastElapsed();

    StepState state();

    List<Throwable> exceptions();
}
