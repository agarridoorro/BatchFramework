package com.ango.batch.chunk.simple;

import com.ango.batch.IChunkStepBuilder;
import com.ango.batch.IStep;
import com.ango.batch.IStepStatus;
import com.ango.batch.StepState;
import com.ango.batch.chunk.FilterModuleProcessor;
import com.ango.batch.chunk.GenerateNumbersReader;
import com.ango.batch.chunk.PrintNumbersWriter;
import com.ango.batch.exceptions.StepExecutionException;
import com.ango.batch.tx.BatchTransactionManager;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ChunkTests
{
    @Test
    void exactChunk()
    {
        long now = System.currentTimeMillis();

        IStep chunkStep = IChunkStepBuilder.<Integer, String>instance()
                .setName("ExactStep")
                .setThrowExceptions(true)
                .setReader(new GenerateNumbersReader().setMax(20))
                .setProcessor(new FilterModuleProcessor().setFilterModule(4))
                .setWriter(new PrintNumbersWriter())
                .setCommitInterval(5)
                .build();

        IStepStatus status = chunkStep.execute();

        assertEquals("ExactStep", status.name());
        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Completed, status.state());
        assertEquals(20, status.read());
        assertEquals(5, status.skipped());
        assertEquals(15, status.written());
        assertEquals(0, status.exceptions().size());
        assertTrue((status.endTime() - status.initTime()) > status.lastElapsed());
        assertEquals(5, status.committed());
    }

    @Test
    void noExactChunk()
    {
        long now = System.currentTimeMillis();

        IStep chunkStep = IChunkStepBuilder.<Integer, String>instance()
                .setName("NoExactStep")
                .setThrowExceptions(true)
                .setReader(new GenerateNumbersReader().setMax(26))
                .setProcessor(new FilterModuleProcessor().setFilterModule(4))
                .setWriter(new PrintNumbersWriter())
                .setCommitInterval(5)
                .build();

        IStepStatus status = chunkStep.execute();

        assertEquals("NoExactStep", status.name());
        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Completed, status.state());
        assertEquals(26, status.read());
        assertEquals(6, status.skipped());
        assertEquals(20, status.written());
        assertEquals(0, status.exceptions().size());
        assertTrue((status.endTime() - status.initTime()) > status.lastElapsed());
        assertEquals(6, status.committed());
    }

    @Test
    void errorRead()
    {
        long now = System.currentTimeMillis();

        IStep chunkStep = IChunkStepBuilder.<Integer, String>instance()
                .setName("Error")
                .setThrowExceptions(false)
                .setReader(new GenerateNumbersReader().setReadsToFail(3).setMax(26))
                .setProcessor(new FilterModuleProcessor().setFilterModule(4))
                .setWriter(new PrintNumbersWriter())
                .setCommitInterval(5)
                .build();

        IStepStatus status = chunkStep.execute();

        assertEquals("Error", status.name());
        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Failed, status.state());
        assertEquals(0, status.read());
        assertEquals(0, status.skipped());
        assertEquals(0, status.written());
        assertEquals(1, status.exceptions().size());
        assertEquals(RuntimeException.class, status.exceptions().get(0).getClass());
        assertTrue((status.endTime() - status.initTime()) > status.lastElapsed());
        assertEquals(0, status.committed());

        chunkStep = IChunkStepBuilder.<Integer, String>instance()
                .setName("Error")
                .setThrowExceptions(true)
                .setReader(new GenerateNumbersReader().setReadsToFail(3).setMax(26))
                .setProcessor(new FilterModuleProcessor().setFilterModule(4))
                .setWriter(new PrintNumbersWriter())
                .setCommitInterval(5)
                .build();

        assertThrows(StepExecutionException.class, chunkStep::execute);

        chunkStep = IChunkStepBuilder.<Integer, String>instance()
                .setName("Error")
                .setThrowExceptions(false)
                .setReader(new GenerateNumbersReader().setReadsToFail(13).setMax(26))
                .setProcessor(new FilterModuleProcessor().setFilterModule(4))
                .setWriter(new PrintNumbersWriter())
                .setCommitInterval(5)
                .build();

        status = chunkStep.execute();

        assertEquals(StepState.Failed, status.state());
        assertEquals(10, status.read());
        assertEquals(2, status.skipped());
        assertEquals(8, status.written());
        assertEquals(1, status.exceptions().size());
        assertEquals(RuntimeException.class, status.exceptions().get(0).getClass());
        assertEquals(2, status.committed());
    }

    @Test
    void errorProcess()
    {
        //First Iteration
        IStep step = IChunkStepBuilder.<Integer, String>instance()
                        .setName("Error")
                        .setReader(new GenerateNumbersReader().setMax(26))
                        .setProcessor(new FilterModuleProcessor().setProcessToFail(2).setFilterModule(4))
                        .setWriter(new PrintNumbersWriter())
                        .setCommitInterval(5)
                        .setThrowExceptions(false)
                        .setTransactionManager(BatchTransactionManager.getInstance())
                        .build();

        IStepStatus status = step.execute();

        assertEquals(StepState.Failed, status.state());
        assertEquals(5, status.read());
        assertEquals(0, status.skipped());
        assertEquals(0, status.written());
        assertEquals(1, status.exceptions().size());
        assertEquals(RuntimeException.class, status.exceptions().get(0).getClass());
        assertEquals(0, status.committed());

        //Third iteration
        step = IChunkStepBuilder.<Integer, String>instance()
                .setName("Error")
                .setReader(new GenerateNumbersReader().setMax(26))
                .setProcessor(new FilterModuleProcessor().setProcessToFail(13).setFilterModule(4))
                .setWriter(new PrintNumbersWriter())
                .setCommitInterval(5)
                .setThrowExceptions(false)
                .setTransactionManager(BatchTransactionManager.getInstance())
                .build();

        status = step.execute();

        assertEquals(StepState.Failed, status.state());
        assertEquals(15, status.read());
        assertEquals(2, status.skipped());
        assertEquals(8, status.written());
        assertEquals(1, status.exceptions().size());
        assertEquals(RuntimeException.class, status.exceptions().get(0).getClass());
        assertEquals(2, status.committed());
    }

    @Test
    void errorWrite()
    {
        //First Iteration
        IStep step = IChunkStepBuilder.<Integer, String>instance()
                        .setName("Error")
                        .setReader(new GenerateNumbersReader().setMax(26))
                        .setProcessor(new FilterModuleProcessor().setFilterModule(4))
                        .setWriter(new PrintNumbersWriter().setWritesToFail(3))
                        .setCommitInterval(5)
                        .setThrowExceptions(false)
                        .setTransactionManager(BatchTransactionManager.getInstance())
                        .build();

        IStepStatus status = step.execute();

        assertEquals(StepState.Failed, status.state());
        assertEquals(5, status.read());
        assertEquals(0, status.skipped());
        assertEquals(0, status.written());
        assertEquals(1, status.exceptions().size());
        assertEquals(RuntimeException.class, status.exceptions().get(0).getClass());
        assertEquals(0, status.committed());

        //Third iteration
        step = IChunkStepBuilder.<Integer, String>instance()
                .setName("Error")
                .setReader(new GenerateNumbersReader().setMax(26))
                .setProcessor(new FilterModuleProcessor().setFilterModule(4))
                .setWriter(new PrintNumbersWriter().setWritesToFail(11))
                .setCommitInterval(5)
                .setThrowExceptions(false)
                .setTransactionManager(BatchTransactionManager.getInstance())
                .build();

        status = step.execute();

        assertEquals(StepState.Failed, status.state());
        assertEquals(15, status.read());
        assertEquals(2, status.skipped());
        assertEquals(8, status.written());
        assertEquals(1, status.exceptions().size());
        assertEquals(RuntimeException.class, status.exceptions().get(0).getClass());
        assertEquals(2, status.committed());
    }
}
