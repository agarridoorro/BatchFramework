package com.ango.batch.chunk.multi;

import com.ango.batch.exceptions.HandShakeException;
import com.ango.batch.exceptions.ProducerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class ProducerData<T>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerData.class);

    private final CyclicBarrier barrier = new CyclicBarrier(2);
    private final int consumersCount;
    private boolean isError = false;
    private final MultiChunkStatus status;
    private final List<List<T>> dataArray ;
    private final int commitInterval;
    private final int waitSeconds;
    private boolean doFinish;
    private int read;
    private final OrchestratorActions orchestratorActions;
    private final ProducerActions producerActions;

    public ProducerData(MultiChunkStatus status, int consumersCount, int commitInterval, int waitSeconds)
    {
        this.status = status;
        this.consumersCount = consumersCount;
        this.commitInterval = commitInterval;
        this.waitSeconds = waitSeconds;
        orchestratorActions = new OrchestratorActions();
        producerActions = new ProducerActions();
        this.doFinish = false;
        this.read = 0;
        dataArray = new ArrayList<>(consumersCount);
        for (int i = 0; i < consumersCount; i++)
        {
            dataArray.add(new ArrayList<>(commitInterval));
        }
    }

    public MultiChunkStatus status()
    {
        return status;
    }

    private boolean handShakeForNextPhase()
    {
        try
        {
            if (waitSeconds > 0)
            {
                barrier.await(waitSeconds, TimeUnit.SECONDS);
            }
            else
            {
                barrier.await();
            }
            return true;
        }
        catch (InterruptedException | BrokenBarrierException | TimeoutException e)
        {
            LOGGER.error("Exception in handshake {}", e.getClass());
            isError = true;
            status.add(new HandShakeException(e));
            return false;
        }
    }

    public OrchestratorActions orchestratorActions()
    {
        return orchestratorActions;
    }

    public ProducerActions producerActions() { return producerActions; }

    public class OrchestratorActions
    {
        private OrchestratorActions() { }

        public List<T> data(int index)
        {
            checkIndex(index);
            return dataArray.get(index);
        }

        public boolean isError() { return isError; }

        public int read() { return read; }

        public void startRead()
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Order producer to read data");
            handShakeForNextPhase();
        }

        public void waitForData()
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Waiting for data");
            handShakeForNextPhase();
        }

        public void finishProducer()
        {
            if (!doFinish)
            {
                if (LOGGER.isDebugEnabled()) LOGGER.debug("Order producer to finish");
                doFinish = true;
                handShakeForNextPhase();
            }
        }
    }

    public class ProducerActions
    {
        private ProducerActions() { }

        public void clear(int index)
        {
            checkIndex(index);
            dataArray.get(index).clear();
        }
        
        public void addData(int index, T data)
        {
            checkIndex(index);
            dataArray.get(index).add(data);
        }

        public void add(Throwable t)
        {
            isError = true;
            status.add(new ProducerException(t));
        }

        public void totalRead(int size)
        {
            read = size;
        }

        public int commitInterval() { return  commitInterval; }

        public int consumersCount() { return consumersCount; }

        public boolean waitForQueryData()
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Waiting for querying data");
            return handShakeForNextPhase() && !doFinish;
        }

        public void dataReady()
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Data ready");
            handShakeForNextPhase();
        }
    }

    private void checkIndex(int index)
    {
        if (index >= consumersCount)
        {
            throw new IndexOutOfBoundsException("Max index [" + (consumersCount - 1) + "] current [" + index + "]");
        }
    }
}
