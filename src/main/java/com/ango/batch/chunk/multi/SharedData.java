package com.ango.batch.chunk.multi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class SharedData<T>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SharedData.class);

    private ConsumerPhase phase;
    private final CyclicBarrier barrier = new CyclicBarrier(2);
    private boolean isError = false;
    private final MultiChunkStatus status;
    private final int index;
    private final List<T> items;
    private boolean doFinish;
    private boolean doRollback;
    private final int waitSeconds;
    private final ProducerActions producerActions;
    private final ConsumerActions consumerActions;
    private boolean stopChunk = false;

    public SharedData(MultiChunkStatus status, int index, int commitInterval, int waitSeconds)
    {
        this.status = status;
        this.index = index;
        this.phase = ConsumerPhase.WaitForData;
        this.doFinish = false;
        this.doRollback = false;
        this.waitSeconds = waitSeconds;
        this.items = new ArrayList<>(commitInterval);
        this.producerActions = new ProducerActions();
        this.consumerActions = new ConsumerActions();
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
            status.add(e);
            return false;
        }
    }

    public ProducerActions producerActions()
    {
        return producerActions;
    }

    public ConsumerActions consumerActions()
    {
        return consumerActions;
    }

    public class ProducerActions
    {
        private ProducerActions() { }

        public void clearItems() { items.clear(); }

        public void addItem(T item) { items.add(item); };

        public int itemsSize() { return items.size(); }

        public boolean isError() { return isError; }

        public boolean dataReady()
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Data ready {}", index);
            return handShakeForNextPhase();
        }

        public boolean finishConsumer()
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Finish consumer {}", index);
            doFinish = true;
            return handShakeForNextPhase();
        }

        public boolean waitForProcessing()
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Wait for processing {}", index);
            return handShakeForNextPhase();
        }

        public boolean orderCommit(boolean doStopChunk)
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Order commit {}", index);
            stopChunk = doStopChunk;
            if (handShakeForNextPhase()) //Order thread the commit
            {
                return handShakeForNextPhase(); //Wait for the commit
            }
            return false;
        }

        public boolean orderRollback()
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Order rollback {}", index);
            doRollback = true;
            if (handShakeForNextPhase()) //Order thread the rollback
            {
                return handShakeForNextPhase(); //Wait for the rollback
            }
            return false;
        }

        public boolean nextIteration()
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Next iteration {}", index);
            return handShakeForNextPhase(); //Order thread the rollback
        }
    }

    public class ConsumerActions
    {
        private ConsumerActions() { }

        public int index() { return index; }

        public List<T> getItems()
        {
            return items;
        }

        public void setPhase(ConsumerPhase newPhase)
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Phase: {}", newPhase);
            phase = newPhase;
        }

        public ConsumerPhase phase() { return phase; }

        public void add(Throwable t)
        {
            isError = true;
            status.add(index, phase, t);
        }

        public boolean doCommit() { return !doRollback; }

        public boolean doRollback() { return doRollback; }

        public boolean stopChunk() { return stopChunk; }

        public boolean waitForData()
        {
            setPhase(ConsumerPhase.WaitForData);
            return handShakeForNextPhase() && !doFinish;
        }

        public boolean finishProcessing()
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Finish processing {}", index);
            return handShakeForNextPhase();
        }

        public boolean waitForDoingCommitOrRollback()
        {
            setPhase(ConsumerPhase.WaitForCommitOrRollback);
            return handShakeForNextPhase() && !doFinish;
        }

        public boolean finishResolution()
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Finish resolution {}", index);
            return handShakeForNextPhase();
        }

        public boolean waitForFinish()
        {
            setPhase(ConsumerPhase.WaitForFinish);
            return handShakeForNextPhase() && !doFinish;
        }
    }
}
