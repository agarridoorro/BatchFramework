package com.ango.batch.tasklet;

import com.ango.batch.step.CommonStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ango.batch.IStep;
import com.ango.batch.IStepStatus;
import com.ango.batch.ITasklet;
import com.ango.batch.resource.TaskletResource;

public class TaskletStep extends CommonStep<TaskletStepStatus> implements IStep
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskletStep.class);

    private TaskletResource tasklet;
    private boolean isTransactional;

    public TaskletStep()
    {
    }

    public void setTasklet(ITasklet tasklet)
    {
        this.tasklet = new TaskletResource(tasklet);
    }

    public void setTransactional(boolean value)
    {
        isTransactional = value;
    }

    @Override
    public String name()
    {
        return status().name();
    }

    @Override
    public IStepStatus execute()
    {
        status().reset();
        if (isTransactional)
        {
            doExecuteWithTx();
        }
        else
        {
            doExecuteWithoutTx();
        }
        return checkStatus();
    }

    private void doExecuteWithoutTx()
    {
        try
        {
            status().start();
            status().save(); //Save initial state
            tasklet.open(); //Open resource
            tasklet.execute(); //Execute
            tasklet.close(); //Close resource
            status().stop(); //Stop state
            status().save(); //Save final state
        }
        catch (Throwable t)
        {
            LOGGER.error("Error executing step [" + status().name() + "]", t);
            status().stop(t);
            tasklet.tryClose(status());
            status().saveProtected();
        }
    }

    private void doExecuteWithTx()
    {
        try
        {
            status().start();
            status().save(); //Save initial state
            transactionManager().begin();
            try
            {
                tasklet.open();
                tasklet.execute();
                tasklet.close();
                status().stop();
                status().commit();
                status().save(); //Save final state
                transactionManager().commit();
            }
            catch (Throwable t)
            {
                LOGGER.error("Error executing step [" + status().name() + "]", t);
                status().stop(t);
                status().undo();
                tasklet.tryClose(status());
                try
                {
                    transactionManager().rollback();
                }
                catch (Throwable tr)
                {
                    LOGGER.error("Error rolling back step [" + status().name() + "]", tr);
                    status().stop(tr);
                }
                status().saveProtected();
            }
        }
        catch (Throwable t)
        {
            LOGGER.error("Error starting step [" + status().name() + "]", t);
            status().stop(t);
            status().saveProtected();
        }
    }
}
