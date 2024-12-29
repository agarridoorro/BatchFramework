package com.ango.batch.chunk.multi;

import com.ango.batch.*;
import com.ango.batch.exceptions.MultiChunkException;
import com.ango.batch.resource.ProcessorResource;
import com.ango.batch.resource.ReaderResource;
import com.ango.batch.resource.WriterResource;
import com.ango.batch.step.CommonStep;
import com.ango.batch.thread.ExecutorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;

public class MultiChunkStep <T,K> extends CommonStep<MultiChunkStatus> implements IStep
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiChunkStep.class);

    private ReaderResource<T> reader;

    private final List<ProcessorResource<T,K>> processors = new LinkedList<>();

    private final List<WriterResource<K>> writers = new LinkedList<>();

    private final List<SharedData<T>> dataForThreads = new ArrayList<>();

    private int threads;

    private int commitInterval;

    private int timeoutForConsumers;

    public MultiChunkStep()
    {
    }

    public void setReader(IReader<T> reader)
    {
        this.reader = new ReaderResource<>(reader);
    }

    public void addProcessor(IProcessor<T,K> processor)
    {
        processors.add(new ProcessorResource<>(processor));
    }

    public void addWriter(IWriter<K> writer)
    {
        writers.add(new WriterResource<>(writer));
    }

    public void setCommitInterval(int interval)
    {
        this.commitInterval = interval;
    }

    public void setTimeoutForConsumers(int timeout) { this.timeoutForConsumers = timeout; }

    public void setThreads(int threads)
    {
        this.threads = threads;
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
            final int maxItemsToRead = commitInterval * threads;

            status().start();
            status().save();

            openResources();
            startThreads();

            boolean isError = false;
            while (true)
            {
                status().startChunk();

                final List<T> readItems = reader.read(maxItemsToRead);
                final int read = readItems.size();
                status().read(read);

                if (read == 0)
                {
                    stopThreads();
                    break;
                }

                int threadsWithData = putData(readItems);
                int threadIndex = 0;
                int possibleThreadsToFinish = 0;
                for (final SharedData<T> data : dataForThreads)
                {
                    if (threadIndex++ < threadsWithData)
                    {
                        data.producerActions().waitForProcessing();
                        if (data.producerActions().isError())
                        {
                            isError = true;
                        }
                        if (isError) //Do rollback
                        {
                            data.producerActions().orderRollback();
                        }
                        else //Do commit
                        {
                            //Stop chunk if it's the last thread with data
                            data.producerActions().orderCommit(threadIndex == threadsWithData);
                            isError = data.producerActions().isError();
                        }
                        if (isError) //If the chunk is in error, finish the consumer
                        {
                            data.producerActions().finishConsumer();
                        }
                        else if (read < maxItemsToRead) //If in the next iteration there's no data, finish the consumer
                        {
                            data.producerActions().finishConsumer();
                        }
                        else
                        {
                            data.producerActions().nextIteration();
                            possibleThreadsToFinish++;
                        }
                    }
                }

                if (isError)
                {
                    for (int i = 0; i < possibleThreadsToFinish; i++)
                    {
                        dataForThreads.get(i).producerActions().finishConsumer();
                    }
                    break;
                }

                if (read < maxItemsToRead)
                {
                    break;
                }
            }

            closeResources();

            if (isError)
            {
                status().stop(new MultiChunkException("One or more consumers have failed"));
            }
            else
            {
                status().stop();
            }
            try
            {
                status().save();
            }
            catch (Throwable t)
            {
                status().stop();
                status().saveProtected();
            }
        }
        catch (Throwable t)
        {
            LOGGER.error("Error executing step [{}]", status().name(), t);
            status().stop(t);
            stopThreads();
            closeResources();
            status().saveProtected();
        }
    }

    private int putData(List<T> items)
    {
        final int size = items.size();
        int globalIndex = 0;
        int dataWithElementsCounter = 0;
        for (final SharedData<T> data : dataForThreads)
        {
            data.producerActions().clearItems();
            int localIndex = 0;
            while (globalIndex < size && localIndex++ < commitInterval)
            {
                data.producerActions().addItem(items.get(globalIndex++));
            }
            if (data.producerActions().itemsSize() > 0)
            {
                dataWithElementsCounter++;
                data.producerActions().dataReady();
            }
            else
            {
                data.producerActions().finishConsumer();
            }
        }
        return dataWithElementsCounter;
    }

    private void stopThreads()
    {
        for (final SharedData<T> data : dataForThreads)
        {
            data.producerActions().finishConsumer();
        }
    }

    private void startThreads()
    {
        final Executor executor = ExecutorFactory.getInstance(threads, "consumer");
        for (int i = 0; i < threads; i++)
        {
            dataForThreads.add(new SharedData<>(status(), i, commitInterval, timeoutForConsumers));
            final MultiChunkConsumer<T,K> consumer = new MultiChunkConsumer<>(transactionManager(),
                    processors.get(i), writers.get(i), dataForThreads.get(i));
            executor.execute(consumer);
        }
    }

    private void openResources() throws IOException
    {
        reader.open();
        for (IResource resource : processors)
        {
            resource.open();
        }
        for (IResource resource : writers)
        {
            resource.open();
        }
    }

    private void closeResources()
    {
        reader.tryClose(status());
        for (ProcessorResource<T,K> resource : processors)
        {
            resource.tryClose(status());
        }
        for (WriterResource<K> resource : writers)
        {
            resource.tryClose(status());
        }
    }
}
