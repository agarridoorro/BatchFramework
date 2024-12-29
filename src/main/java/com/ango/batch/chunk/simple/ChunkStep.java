package com.ango.batch.chunk.simple;

import java.util.List;

import com.ango.batch.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ango.batch.step.CommonStep;
import com.ango.batch.resource.ProcessorResource;
import com.ango.batch.resource.ReaderResource;
import com.ango.batch.resource.WriterResource;

public class ChunkStep<T,K> extends CommonStep<ChunkStepStatus> implements IStep
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ChunkStep.class);
    private ReaderResource<T> reader;
    private ProcessorResource<T,K> processor;
    private WriterResource<K> writer;
    private int commitInterval;

    public ChunkStep()
    {
    }

    public void setReader(IReader<T> reader)
    {
        this.reader = new ReaderResource<>(reader);
    }

    public void setProcessor(IProcessor<T,K> processor)
    {
        this.processor = new ProcessorResource<>(processor);
    }

    public void setWriter(IWriter<K> writer)
    {
        this.writer = new WriterResource<>(writer);
    }

    public void setCommitInterval(int interval)
    {
        this.commitInterval = interval;
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
        doExecute();
        return checkStatus();
    }

    private void doExecute()
    {
        try
        {
            status().start();
            status().save();

            reader.open();
            processor.open();
            writer.open();

            while (true)
            {
                status().startChunk();

                final List<T> readItems = reader.read(commitInterval);
                final int read = readItems.size();
                status().read(read);

                transactionManager().begin();
                try
                {
                    if (read > 0)
                    {
                        final List<K> itemsProcessed = processor.process(readItems);
                        final int written = itemsProcessed.size();
                        status().skip(read - written);
                        if (written > 0)
                        {
                            writer.write(itemsProcessed);
                        }
                        status().write(written);
                    }
                    status().stopChunk();
                    status().commit();
                    status().save();
                }
                catch (Throwable t)
                {
                    status().stop(t);
                    status().undo();
                    transactionManager().rollback();
                    status().saveProtected();
                    break;
                }

                transactionManager().commit();
                status().consolidate();

                if (read < commitInterval || read == 0)
                {
                    break;
                }
            }

            reader.close();
            processor.close();
            writer.close();

            if (StepState.Executing.equals(status().state()))
            {
                status().stop();
                status().save();
            }
        }
        catch (Throwable t) //Begin tx and before or Rollback catch
        {
            LOGGER.error("Error executing step [" + status().name() + "]", t);
            status().stop(t);
            status().undo();

            reader.tryClose(status());
            processor.tryClose(status());
            writer.tryClose(status());

            status().saveProtected();
        }
    }
}
