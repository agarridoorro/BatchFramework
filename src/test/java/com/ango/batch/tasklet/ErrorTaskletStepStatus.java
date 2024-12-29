package com.ango.batch.tasklet;

import com.ango.batch.exceptions.SaveStateException;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class ErrorTaskletStepStatus extends TaskletStepStatus
{
    private final List<Integer> savesToFail;
    private int numberOfSaves = 0;

    public ErrorTaskletStepStatus(String name, int[] savesToFail)
    {
        super(name);
        this.savesToFail = Arrays.asList(Arrays.stream(savesToFail).boxed().toArray(Integer[]::new));
    }

    @Override
    public void save() throws SaveStateException
    {
        if (savesToFail.contains(++numberOfSaves))
        {
            try
            {
                throw new SQLException("Fail on [" + numberOfSaves + "] save");
            }
            catch (Throwable t)
            {
                throw new SaveStateException("Error persisting step state  [" + name() + "]", t);
            }
        }

        super.save();
    }
}
