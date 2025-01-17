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

    private final List<ConsumerData<T>> exchangeConsumersData = new ArrayList<>();

    private ProducerData<T> exchangeProducerData;

    private int consumers;

    private int commitInterval;

    private int waitTimeout;

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

    public void setWaitTimeout(int timeout) { this.waitTimeout = timeout; }

    public void setConsumers(int consumers)
    {
        this.consumers = consumers;
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
            final int maxItemsToRead = commitInterval * consumers;

            status().start();
            status().save();

            openResources();
            startConsumers();
            startProducer();

            exchangeProducerData.orchestratorActions().startRead();

            boolean isErrorInProducer;
            boolean isErrorInConsumers = false;
            while (true)
            {
                status().startChunk();

                exchangeProducerData.orchestratorActions().waitForData();
                isErrorInProducer = exchangeProducerData.orchestratorActions().isError();
                final int read = exchangeProducerData.orchestratorActions().read();
                if (read == 0 || isErrorInProducer)
                {
                    stopProducer();
                    stopConsumers();
                    break;
                }

                int consumersWithData = putDataToConsumers();
                if (read < maxItemsToRead)
                {
                    stopProducer();
                }
                else
                {
                    exchangeProducerData.orchestratorActions().startRead();
                }

                for (int consumerIndex = 0; consumerIndex < consumersWithData; consumerIndex++)
                {
                    final ConsumerData<T> data = exchangeConsumersData.get(consumerIndex);
                    data.orchestratorActions().waitForProcessing();
                    if (data.orchestratorActions().isError())
                    {
                        isErrorInConsumers = true;
                    }
                    if (isErrorInConsumers) //Do rollback
                    {
                        data.orchestratorActions().doRollback();
                        data.orchestratorActions().waitForResolution();
                    }
                    else //Do commit
                    {
                        //Stop chunk if it's the last thread with data
                        final boolean isLastConsumerWithData = consumerIndex + 1 == consumersWithData;
                        data.orchestratorActions().doCommit(isLastConsumerWithData);
                        data.orchestratorActions().waitForResolution();
                        isErrorInConsumers = data.orchestratorActions().isError();
                    }
                    if (isErrorInConsumers || (read < maxItemsToRead)) //If the chunk is in error or there's no data for next iteration, finish the consumer
                    {
                        data.orchestratorActions().finishConsumer();
                    }
                }

                if (isErrorInConsumers)
                {
                    stopProducer();
                    stopConsumers();
                    break;
                }

                if (read < maxItemsToRead)
                {
                    break;
                }
            }

            closeResources();

            if (isErrorInProducer)
            {
                status().stop(new MultiChunkException("The producer has failed"));
            }
            else if (isErrorInConsumers)
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
            stopProducer();
            stopConsumers();
            closeResources();
            status().saveProtected();
        }
    }

    private int putDataToConsumers()
    {
        int consumersWithDataCounter = 0;
        for (int consumerIndex = 0; consumerIndex < consumers; consumerIndex++)
        {
            final ConsumerData<T> exchangeDataI = exchangeConsumersData.get(consumerIndex);
            final List<T> dataForConsumerI = exchangeProducerData.orchestratorActions().data(consumerIndex);
            if (dataForConsumerI.isEmpty())
            {
                exchangeDataI.orchestratorActions().finishConsumer();
            }
            else
            {
                consumersWithDataCounter++;
                exchangeDataI.orchestratorActions().addItems(dataForConsumerI);
                exchangeDataI.orchestratorActions().dataReady();
            }
        }
        return consumersWithDataCounter;
    }

    private void stopProducer()
    {
        exchangeProducerData.orchestratorActions().finishProducer();
    }

    private void stopConsumers()
    {
        for (final ConsumerData<T> data : exchangeConsumersData)
        {
            data.orchestratorActions().finishConsumer();
        }
    }

    private void startProducer()
    {
        final Executor executor = ExecutorFactory.getInstance(consumers, "producer");
        exchangeProducerData = new ProducerData<>(status(), consumers, commitInterval, waitTimeout);
        final MultiChunkProducer<T> producer = new MultiChunkProducer<>(reader, exchangeProducerData);
        executor.execute(producer);
    }

    private void startConsumers()
    {
        final Executor executor = ExecutorFactory.getInstance(consumers, "consumer");
        for (int i = 0; i < consumers; i++)
        {
            exchangeConsumersData.add(new ConsumerData<>(status(), i, commitInterval, waitTimeout));
            final MultiChunkConsumer<T,K> consumer = new MultiChunkConsumer<>(transactionManager(),
                    processors.get(i), writers.get(i), exchangeConsumersData.get(i));
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
