package com.ango.batch.chunk.multi;

import com.ango.batch.IMultiChunkStepBuilder;
import com.ango.batch.IStep;
import com.ango.batch.IStepStatus;
import com.ango.batch.StepState;
import com.ango.batch.chunk.FilterModuleProcessor;
import com.ango.batch.chunk.GenerateNumbersReader;
import com.ango.batch.chunk.PrintNumbersWriter;
import com.ango.batch.common.ErrorTransactionManager;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MultiChunkTests
{
    @Test
    void exactChunk()
    {
        //Two chunks, exact items for each thread
        long now = System.currentTimeMillis();

        IStep chunkStep = IMultiChunkStepBuilder.<Integer, String>instance()
                .setName("ExactStep")
                .setConsumers(2)
                .setCommitInterval(5)
                .setThrowExceptions(true)
                //Producer
                .setReader(new GenerateNumbersReader().setMax(20))
                //Consumer1
                .addProcessor(new FilterModuleProcessor().setFilterModule(4))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                //Consumer2
                .addProcessor(new FilterModuleProcessor().setFilterModule(3))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
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
        assertEquals(4, status.committed());
    }

    @Test
    void noExactChunk()
    {
        //Two chunks, in second chunk only thread 1 has data
        long now = System.currentTimeMillis();

        IStep chunkStep = IMultiChunkStepBuilder.<Integer, String>instance()
                .setName("NoExactStep")
                .setConsumers(2)
                .setCommitInterval(5)
                .setThrowExceptions(true)
                //Producer
                .setReader(new GenerateNumbersReader().setMax(23))
                //Consumer1
                .addProcessor(new FilterModuleProcessor().setFilterModule(4))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                //Consumer2
                .addProcessor(new FilterModuleProcessor().setFilterModule(3))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                .build();

        IStepStatus status = chunkStep.execute();

        assertEquals("NoExactStep", status.name());
        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Completed, status.state());
        assertEquals(23, status.read());
        assertEquals(5, status.skipped());
        assertEquals(18, status.written());
        assertEquals(0, status.exceptions().size());
        assertTrue((status.endTime() - status.initTime()) > status.lastElapsed());
        assertEquals(5, status.committed());

        //Two chunks, in second chunk two threads have data
        now = System.currentTimeMillis();

        chunkStep = IMultiChunkStepBuilder.<Integer, String>instance()
                .setName("NoExactStep")
                .setConsumers(2)
                .setCommitInterval(5)
                .setThrowExceptions(true)
                //Producer
                .setReader(new GenerateNumbersReader().setMax(27))
                //Consumer1
                .addProcessor(new FilterModuleProcessor().setFilterModule(4))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                //Consumer2
                .addProcessor(new FilterModuleProcessor().setFilterModule(3))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                .build();

        status = chunkStep.execute();

        assertEquals("NoExactStep", status.name());
        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Completed, status.state());
        assertEquals(27, status.read());
        assertEquals(7, status.skipped());
        assertEquals(20, status.written());
        assertEquals(0, status.exceptions().size());
        assertTrue((status.endTime() - status.initTime()) > status.lastElapsed());
        assertEquals(6, status.committed());
    }

    @Test
    void readError()
    {
        //Read fail in first chunk
        long now = System.currentTimeMillis();

        IStep chunkStep = IMultiChunkStepBuilder.<Integer, String>instance()
                .setName("Error")
                .setConsumers(2)
                .setCommitInterval(5)
                .setThrowExceptions(false)
                //Producer
                .setReader(new GenerateNumbersReader().setReadsToFail(7).setMax(20))
                //Consumer1
                .addProcessor(new FilterModuleProcessor().setFilterModule(4))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                //Consumer2
                .addProcessor(new FilterModuleProcessor().setFilterModule(3))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                .build();

        IStepStatus status = chunkStep.execute();

        assertEquals("Error", status.name());
        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Failed, status.state());
        assertEquals(0, status.read());
        assertEquals(0, status.skipped());
        assertEquals(0, status.written());
        assertEquals(2, status.exceptions().size());
        assertTrue((status.endTime() - status.initTime()) > status.lastElapsed());
        assertEquals(0, status.committed());

        //Read fail in second chunk
        now = System.currentTimeMillis();

        chunkStep = IMultiChunkStepBuilder.<Integer, String>instance()
                .setName("Error")
                .setConsumers(2)
                .setCommitInterval(5)
                .setThrowExceptions(false)
                //Producer
                .setReader(new GenerateNumbersReader().setReadsToFail(13).setMax(20))
                //Consumer1
                .addProcessor(new FilterModuleProcessor().setFilterModule(4))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                //Consumer2
                .addProcessor(new FilterModuleProcessor().setFilterModule(3))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                .build();

        status = chunkStep.execute();

        assertEquals("Error", status.name());
        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Failed, status.state());
        assertEquals(10, status.read());
        assertEquals(3, status.skipped());
        assertEquals(7, status.written());
        assertEquals(2, status.exceptions().size());
        assertTrue((status.endTime() - status.initTime()) > status.lastElapsed());
        assertEquals(2, status.committed());
    }

    @Test
    void processErrorConsumer2()
    {
        //One chunk, the first thread processes ok and the second one fails
        long now = System.currentTimeMillis();

        IStep chunkStep = IMultiChunkStepBuilder.<Integer, String>instance()
                .setName("processErrorConsumer2")
                .setConsumers(2)
                .setCommitInterval(5)
                .setThrowExceptions(false)
                //Producer
                .setReader(new GenerateNumbersReader().setMax(20))
                //Consumer1
                .addProcessor(new FilterModuleProcessor().setFilterModule(4))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                //Consumer2
                .addProcessor(new FilterModuleProcessor().setProcessToFail(2).setFilterModule(3))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                .build();

        IStepStatus status = chunkStep.execute();

        assertEquals("processErrorConsumer2", status.name());
        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Failed, status.state());
        assertTrue(status.read() >= 10);
        assertEquals(1, status.skipped());
        assertEquals(4, status.written());
        assertEquals(2, status.exceptions().size());
        assertTrue((status.endTime() - status.initTime()) > status.lastElapsed());
        assertEquals(1, status.committed());
    }

    @Test
    void processErrorConsumer1()
    {
        //One chunk, the first thread fails and the second one processes ok but has to rollback
        long now = System.currentTimeMillis();

        IStep chunkStep = IMultiChunkStepBuilder.<Integer, String>instance()
                .setName("processErrorConsumer2")
                .setConsumers(2)
                .setCommitInterval(5)
                .setThrowExceptions(false)
                //Producer
                .setReader(new GenerateNumbersReader().setMax(20))
                //Consumer1
                .addProcessor(new FilterModuleProcessor().setProcessToFail(2).setFilterModule(4))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                //Consumer2
                .addProcessor(new FilterModuleProcessor().setFilterModule(3))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                .build();

        IStepStatus status = chunkStep.execute();

        assertEquals("processErrorConsumer2", status.name());
        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Failed, status.state());
        assertTrue(status.read() >= 10);
        assertEquals(0, status.skipped());
        assertEquals(0, status.written());
        assertEquals(2, status.exceptions().size());
        assertTrue((status.endTime() - status.initTime()) > status.lastElapsed());
        assertEquals(0, status.committed());
    }

    @Test
    void errorTimeout()
    {
        //One chunk, the first thread fails for timeout, the second one has to rollback
        long now = System.currentTimeMillis();

        IStep chunkStep = IMultiChunkStepBuilder.<Integer, String>instance()
                .setName("errorTimeout")
                .setConsumers(2)
                .setCommitInterval(5)
                .setWaitTimeout(1)
                .setThrowExceptions(false)
                //Producer
                .setReader(new GenerateNumbersReader().setMax(20))
                //Consumer1
                .addProcessor(new FilterModuleProcessor().setFilterModule(4))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5000))
                //Consumer2
                .addProcessor(new FilterModuleProcessor().setFilterModule(3))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                .build();

        IStepStatus status = chunkStep.execute();

        assertEquals("errorTimeout", status.name());
        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Failed, status.state());
        assertTrue(status.read() >= 10);
        assertEquals(0, status.skipped());
        assertEquals(0, status.written());
        assertTrue(status.exceptions().size() >= 2);
        assertTrue((status.endTime() - status.initTime()) > status.lastElapsed());
        assertEquals(0, status.committed());
    }

    @Test
    void errorBegin()
    {
        //First chunk, first thread fails
        long now = System.currentTimeMillis();

        IStep chunkStep = IMultiChunkStepBuilder.<Integer, String>instance()
                .setName("error")
                .setConsumers(2)
                .setCommitInterval(5)
                .setThrowExceptions(false)
                .setTransactionManager(new ErrorTransactionManager().setWaitDuration(1000).setWaitThread("consumer-1").setBeginsToFail(1))
                //Producer
                .setReader(new GenerateNumbersReader().setMax(20))
                //Consumer1
                .addProcessor(new FilterModuleProcessor().setFilterModule(4))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                //Consumer2
                .addProcessor(new FilterModuleProcessor().setFilterModule(3))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                .build();

        IStepStatus status = chunkStep.execute();

        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Failed, status.state());
        assertTrue(status.read() >= 10);
        assertEquals(0, status.skipped());
        assertEquals(0, status.written());
        assertEquals(2, status.exceptions().size());
        assertTrue((status.endTime() - status.initTime()) > status.lastElapsed());
        assertEquals(0, status.committed());

        //First chunk, second thread fails
        now = System.currentTimeMillis();

        chunkStep = IMultiChunkStepBuilder.<Integer, String>instance()
                .setName("error")
                .setConsumers(2)
                .setCommitInterval(5)
                .setThrowExceptions(false)
                .setTransactionManager(new ErrorTransactionManager().setWaitDuration(1000).setWaitThread("consumer-0").setBeginsToFail(1))
                //Producer
                .setReader(new GenerateNumbersReader().setMax(20))
                //Consumer1
                .addProcessor(new FilterModuleProcessor().setFilterModule(4))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                //Consumer2
                .addProcessor(new FilterModuleProcessor().setFilterModule(3))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                .build();

        status = chunkStep.execute();

        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Failed, status.state());
        assertTrue(status.read() >= 10);
        assertEquals(1, status.skipped());
        assertEquals(4, status.written());
        assertEquals(2, status.exceptions().size());
        assertTrue((status.endTime() - status.initTime()) > status.lastElapsed());
        assertEquals(1, status.committed());
    }

    @Test
    void errorCommit()
    {
        //First chunk, first thread fails
        long now = System.currentTimeMillis();

        IStep chunkStep = IMultiChunkStepBuilder.<Integer, String>instance()
                .setName("error")
                .setConsumers(2)
                .setCommitInterval(5)
                .setThrowExceptions(false)
                .setTransactionManager(new ErrorTransactionManager().setCommitsToFail(1))
                //Producer
                .setReader(new GenerateNumbersReader().setMax(20))
                //Consumer1
                .addProcessor(new FilterModuleProcessor().setFilterModule(4))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                //Consumer2
                .addProcessor(new FilterModuleProcessor().setFilterModule(3))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                .build();

        IStepStatus status = chunkStep.execute();

        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Failed, status.state());
        assertTrue(status.read() >= 10);
        assertEquals(0, status.skipped());
        assertEquals(0, status.written());
        assertEquals(2, status.exceptions().size());
        assertTrue((status.endTime() - status.initTime()) > status.lastElapsed());
        assertEquals(0, status.committed());

        //First chunk, second thread fails
        now = System.currentTimeMillis();

        chunkStep = IMultiChunkStepBuilder.<Integer, String>instance()
                .setName("error")
                .setConsumers(2)
                .setCommitInterval(5)
                .setThrowExceptions(false)
                .setTransactionManager(new ErrorTransactionManager().setCommitsToFail(2))
                //Producer
                .setReader(new GenerateNumbersReader().setMax(20))
                //Consumer1
                .addProcessor(new FilterModuleProcessor().setFilterModule(4))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                //Consumer2
                .addProcessor(new FilterModuleProcessor().setFilterModule(3))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                .build();

        status = chunkStep.execute();

        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Failed, status.state());
        assertTrue(status.read() >= 10);
        assertEquals(1, status.skipped());
        assertEquals(4, status.written());
        assertEquals(2, status.exceptions().size());
        assertTrue((status.endTime() - status.initTime()) >= status.lastElapsed());
        assertEquals(1, status.committed());
    }

    @Test
    void errorRollback()
    {
        //First chunk, first thread fails
        long now = System.currentTimeMillis();

        IStep chunkStep = IMultiChunkStepBuilder.<Integer, String>instance()
                .setName("error")
                .setConsumers(2)
                .setCommitInterval(5)
                .setThrowExceptions(false)
                .setTransactionManager(new ErrorTransactionManager().setRollbacksToFail(1))
                //Producer
                .setReader(new GenerateNumbersReader().setMax(20))
                //Consumer1
                .addProcessor(new FilterModuleProcessor().setProcessToFail(2).setFilterModule(4))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                //Consumer2
                .addProcessor(new FilterModuleProcessor().setFilterModule(3))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                .build();

        IStepStatus status = chunkStep.execute();

        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Failed, status.state());
        assertTrue(status.read() >= 10);
        assertEquals(0, status.skipped());
        assertEquals(0, status.written());
        assertEquals(3, status.exceptions().size());
        assertTrue((status.endTime() - status.initTime()) > status.lastElapsed());
        assertEquals(0, status.committed());

        //First chunk, second thread fails
        now = System.currentTimeMillis();

        chunkStep = IMultiChunkStepBuilder.<Integer, String>instance()
                .setName("error")
                .setConsumers(2)
                .setCommitInterval(5)
                .setThrowExceptions(false)
                .setTransactionManager(new ErrorTransactionManager().setRollbacksToFail(1))
                //Producer
                .setReader(new GenerateNumbersReader().setMax(20))
                //Consumer1
                .addProcessor(new FilterModuleProcessor().setFilterModule(4))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                //Consumer2
                .addProcessor(new FilterModuleProcessor().setProcessToFail(2).setFilterModule(3))
                .addWriter(new PrintNumbersWriter().setMaxMillisToWrite(5))
                .build();

        status = chunkStep.execute();

        assertTrue(status.initTime() >= now);
        assertTrue(status.endTime() > status.initTime());
        assertEquals(StepState.Failed, status.state());
        assertTrue(status.read() >= 10);
        assertEquals(1, status.skipped());
        assertEquals(4, status.written());
        assertEquals(3, status.exceptions().size());
        assertTrue((status.endTime() - status.initTime()) >= status.lastElapsed());
        assertEquals(1, status.committed());
    }
}
