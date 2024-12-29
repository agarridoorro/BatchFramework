package com.ango.batch;

public enum StepState
{
    Starting(1),
    Executing(2),
    Completed(3),
    Failed(4),
    CriticalFailed(5);

    private final int value;

    StepState(int value)
    {
        this.value = value;
    }

    public int value()
    {
        return value;
    }

    public boolean isFailed()
    {
        return value > Completed.value;
    }
}
