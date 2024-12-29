package com.ango.batch.status;

import com.ango.batch.IStepStatus;
import com.ango.batch.StepState;
import com.ango.batch.exceptions.SaveStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class CommonStatus implements IStepStatus, IExceptionVault
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CommonStatus.class);

    private final String name;
    private StepState state;
    private long initTime = 0;
    private long endTime = 0;
    private final List<Throwable> exceptions = new CopyOnWriteArrayList<>();

    public CommonStatus(String name)
    {
        this.name = name;
        this.state = StepState.Starting;
    }

    public void reset()
    {
        state = StepState.Starting;
        initTime = 0;
        endTime = 0;
        exceptions.clear();
    }

    public void start()
    {
        initTime = System.currentTimeMillis();
        state = StepState.Executing;
    }

    public void stop()
    {
        stop(StepState.Completed);
    }

    private void stop(SaveStateException e)
    {
        stop(StepState.CriticalFailed);
        exceptions.add(e);
    }

    public void stop(Throwable e)
    {
        stop(StepState.Failed);
        exceptions.add(e);
    }

    @Override
    public void add(Throwable e)
    {
        stop(e);
    }

    @Override
    public void addAll(List<Throwable> errors)
    {
        exceptions.addAll(errors);
    }

    private void stop(StepState state)
    {
        if (endTime == 0)
        {
            endTime = System.currentTimeMillis();
        }
        this.state = state;
    }

    public void save() throws SaveStateException
    {
        try
        {
            //TODO: insert or update
            LOGGER.debug("Persisting state: " + this);
        }
        catch (Throwable t)
        {
            LOGGER.error("Error persisting step state [" + name + "]", t);
            throw new SaveStateException("Error persisting step state  [" + name + "]", t);
        }
    }

	public void saveProtected()
	{
		try
		{
            if (state == StepState.CriticalFailed)
            {
                state = StepState.Failed;
            }
			save();
		}
		catch (SaveStateException se)
		{
			stop(se);
		}
	}

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public abstract int read();

    @Override
    public abstract int skipped();

    @Override
    public abstract int written();

    @Override
    public long initTime()
    {
        return initTime;
    }

    @Override
    public long endTime()
    {
        return endTime;
    }

    @Override
    public abstract long lastElapsed();

    @Override
    public List<Throwable> exceptions()
    {
        return exceptions;
    }

    @Override
    public StepState state()
    {
        return state;
    }

    @Override
    public String toString()
    {
        return "name [" + name() + "] state [" + state() + "] initTime [" + initTime() + "] endTime [" + endTime() +
         "] lastElapsed [" + lastElapsed() + "] read [" + read() + "] skipped [" + skipped() + "] written [" + written() +
         "] exceptions [" + exceptions().size() + "]";
    }
}
