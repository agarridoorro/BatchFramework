package com.ango.batch.tasklet;

import com.ango.batch.IStep;
import com.ango.batch.IStepStatus;
import com.ango.batch.ITaskletStepBuilder;
import com.ango.batch.StepState;
import com.ango.batch.common.ErrorTransactionManager;
import com.ango.batch.exceptions.SaveStateException;
import com.ango.batch.exceptions.StepExecutionException;
import com.ango.batch.tx.BatchTransactionManager;
import org.junit.jupiter.api.Test;

import javax.transaction.SystemException;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class TaskletTests
{
    @Test
    void testNoTX()
    {
        long now = System.currentTimeMillis();

        IStep taskletStep = ITaskletStepBuilder.instance()
                .setName("DoNothing")
                .setThrowExceptions(true)
                .setTransactional(false)
                .setTasklet(new DoNothingTasklet(false, false, false))
                .build();

        IStepStatus status = taskletStep.execute();

        assertEquals("DoNothing", status.name());
        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Completed, status.state());
        assertEquals(0, status.read());
        assertEquals(0, status.skipped());
        assertEquals(0, status.written());
        assertEquals(0, status.exceptions().size());
        assertEquals(status.lastElapsed(), status.endTime() - status.initTime());
        assertEquals(0, status.committed());
    }

    @Test
    void testTX()
    {
        long now = System.currentTimeMillis();

        IStep taskletStep = ITaskletStepBuilder.instance()
                .setName("DoNothing")
                .setThrowExceptions(false)
                .setTransactional(true)
                .setTasklet(new DoNothingTasklet(false, false, false))
                .build();

        IStepStatus status = taskletStep.execute();

        assertEquals("DoNothing", status.name());
        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Completed, status.state());
        assertEquals(0, status.read());
        assertEquals(0, status.skipped());
        assertEquals(0, status.written());
        assertEquals(0, status.exceptions().size());
        assertEquals(status.lastElapsed(), status.endTime() - status.initTime());
        assertEquals(1, status.committed());
    }

    @Test
    void testError()
    {
        long now = System.currentTimeMillis();

        IStep taskletStep = ITaskletStepBuilder.instance()
                .setName("Error")
                .setThrowExceptions(false)
                .setTransactional(false)
                .setTasklet(new DoNothingTasklet(true, false, false))
                .build();

        IStepStatus status = taskletStep.execute();

        assertEquals("Error", status.name());
        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Failed, status.state());
        assertEquals(0, status.read());
        assertEquals(0, status.skipped());
        assertEquals(0, status.written());
        assertEquals(1, status.exceptions().size());
        assertEquals(RuntimeException.class, status.exceptions().get(0).getClass());
        assertEquals(status.lastElapsed(), status.endTime() - status.initTime());
        assertEquals(0, status.committed());

        IStep taskletStepThrow = ITaskletStepBuilder.instance()
                .setName("Error")
                .setThrowExceptions(true)
                .setTasklet(new DoNothingTasklet(true, false, false))
                .build();

        assertThrows(StepExecutionException.class, taskletStepThrow::execute);
    }

    @Test
    void testErrorTXManager()
    {
        final TaskletStep step = new TaskletStep();
        step.setStatus(new TaskletStepStatus("Error"));
        step.setThrowExceptions(false);
        step.setTransactional(true);

        //Fail on begin tx
        step.setTasklet(new DoNothingTasklet(false, false, false));
        step.setTransactionManager(new ErrorTransactionManager().setBeginsToFail(1));
        IStepStatus status = step.execute();

        assertEquals(StepState.Failed, status.state());
        assertEquals(1, status.exceptions().size());
        assertEquals(SystemException.class, status.exceptions().get(0).getClass());
        assertEquals(0, status.committed());

        //Fail on commit tx
        step.setTransactionManager(new ErrorTransactionManager().setCommitsToFail(1));
        status = step.execute();

        assertEquals(StepState.Failed, status.state());
        assertEquals(1, status.exceptions().size());
        assertEquals(SystemException.class, status.exceptions().get(0).getClass());
        assertEquals(0, status.committed());

        //Fail on rollback
        step.setTransactionManager(new ErrorTransactionManager().setRollbacksToFail(1));
        step.setTasklet(new DoNothingTasklet(true, false, false));
        status = step.execute();

        assertEquals(StepState.Failed, status.state());
        assertEquals(2, status.exceptions().size());
        assertEquals(RuntimeException.class, status.exceptions().get(0).getClass());
        assertEquals(SystemException.class, status.exceptions().get(1).getClass());
        assertEquals(0, status.committed());
    }

    @Test
    void testErrorResources()
    {
        final TaskletStep step = new TaskletStep();
        step.setStatus(new TaskletStepStatus("Error"));
        step.setThrowExceptions(false);
        step.setTransactional(true);
        step.setTransactionManager(BatchTransactionManager.getInstance());

        //Fail on open tasklet
        step.setTasklet(new DoNothingTasklet(false, true, false));
        IStepStatus status = step.execute();

        assertEquals(StepState.Failed, status.state());
        assertEquals(1, status.exceptions().size());
        assertEquals(IOException.class, status.exceptions().get(0).getClass());
        assertEquals(0, status.committed());

        //Fail on close tasklet before commit
        step.setTasklet(new DoNothingTasklet(false,false, true));
        status = step.execute();

        assertEquals(StepState.Failed, status.state());
        assertEquals(2, status.exceptions().size()); //2, 1 in close and 1 in tryClose
        assertEquals(IOException.class, status.exceptions().get(0).getClass());
        assertEquals(0, status.committed());

        //Fail on close tasklet before rollback
        step.setTasklet(new DoNothingTasklet(true, false, true));
        status = step.execute();

        assertEquals(StepState.Failed, status.state());
        assertEquals(2, status.exceptions().size());
        assertEquals(RuntimeException.class, status.exceptions().get(0).getClass());
        assertEquals(IOException.class, status.exceptions().get(1).getClass());
        assertEquals(0, status.committed());
    }

    @Test
    void testErrorSaveState()
    {
        final TaskletStep step = new TaskletStep();
        step.setThrowExceptions(false);
        step.setTransactional(true);
        step.setTransactionManager(BatchTransactionManager.getInstance());

        //Fail on save state starting
        step.setStatus(new ErrorTaskletStepStatus("Error", new int[] {1})); //Only first save
        step.setTasklet(new DoNothingTasklet(false, false, false));
        IStepStatus status = step.execute();

        assertEquals(StepState.Failed, status.state());
        assertEquals(1, status.exceptions().size());
        assertEquals(SaveStateException.class, status.exceptions().get(0).getClass());
        assertEquals(0, status.committed());

        //Fail on save state starting and last save
        step.setStatus(new ErrorTaskletStepStatus("Error", new int[] {1, 2})); //First and second save
        step.setTasklet(new DoNothingTasklet(false, false, false));
        status = step.execute();

        assertEquals(StepState.CriticalFailed, status.state());
        assertEquals(2, status.exceptions().size());
        assertEquals(SaveStateException.class, status.exceptions().get(0).getClass());
        assertEquals(SaveStateException.class, status.exceptions().get(1).getClass());
        assertEquals(0, status.committed());

        //Fail only on save state before commit
        step.setStatus(new ErrorTaskletStepStatus("Error", new int[] {2})); //Second save
        step.setTasklet(new DoNothingTasklet(false, false, false));
        status = step.execute();

        assertEquals(StepState.Failed, status.state());
        assertEquals(1, status.exceptions().size());
        assertEquals(SaveStateException.class, status.exceptions().get(0).getClass());
        assertEquals(0, status.committed());

        //Fail on save state before commit and save state in rollback
        step.setStatus(new ErrorTaskletStepStatus("Error", new int[] {2, 3})); //2 and 3 save
        step.setTasklet(new DoNothingTasklet(false, false, false));
        status = step.execute();

        assertEquals(StepState.CriticalFailed, status.state());
        assertEquals(2, status.exceptions().size());
        assertEquals(SaveStateException.class, status.exceptions().get(0).getClass());
        assertEquals(SaveStateException.class, status.exceptions().get(0).getClass());
        assertEquals(0, status.committed());
    }
}
