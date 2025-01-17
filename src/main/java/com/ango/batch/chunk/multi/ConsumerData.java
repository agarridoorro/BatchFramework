package com.ango.batch.chunk.multi;

import com.ango.batch.exceptions.HandShakeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class ConsumerData<T>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerData.class);

    private ConsumerPhase phase;
    private final CyclicBarrier barrier = new CyclicBarrier(2);
    private boolean isError = false;
    private final MultiChunkStatus status;
    private final int index;
    private final List<T> items;
    private boolean doFinish;
    private boolean doCommit;
    private final int waitSeconds;
    private final OrchestratorActions orchestratorActions;
    private final ConsumerActions consumerActions;
    private boolean stopChunk = false;

    public ConsumerData(MultiChunkStatus status, int index, int commitInterval, int waitSeconds)
    {
        this.status = status;
        this.index = index;
        this.phase = ConsumerPhase.WaitForData;
        this.doFinish = false;
        this.doCommit = false;
        this.waitSeconds = waitSeconds;
        this.items = new ArrayList<>(commitInterval);
        this.orchestratorActions = new OrchestratorActions();
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
            status.add(new HandShakeException(e));
            return false;
        }
    }

    public OrchestratorActions orchestratorActions()
    {
        return orchestratorActions;
    }

    public ConsumerActions consumerActions()
    {
        return consumerActions;
    }

    public class OrchestratorActions
    {
        private OrchestratorActions() { }

        public void addItems(List<T> newItems)
        {
            items.clear();
            items.addAll(newItems);
        };

        public boolean isError() { return isError; }

        public void dataReady()
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Data ready {}", index);
            handShakeForNextPhase();
        }

        public void finishConsumer()
        {
            if (!doFinish)
            {
                if (LOGGER.isDebugEnabled()) LOGGER.debug("Order consumer to Finish {}", index);
                doFinish = true;
                handShakeForNextPhase();
            }
        }

        public void waitForProcessing()
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Wait for processing {}", index);
            handShakeForNextPhase();
        }

        public void doCommit(boolean doStopChunk)
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Order commit {}", index);
            stopChunk = doStopChunk;
            doCommit = true;
            handShakeForNextPhase();
        }

        public void waitForResolution()
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Waiting for resolution {}", index);
            handShakeForNextPhase();
        }

        public void doRollback()
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Order rollback {}", index);
            doCommit = false;
            handShakeForNextPhase();
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

        public boolean doCommit() { return doCommit; }

        public boolean stopChunk() { return stopChunk; }

        public boolean waitForData()
        {
            setPhase(ConsumerPhase.WaitForData);
            return handShakeForNextPhase() && !doFinish;
        }

        public void finishProcessing()
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Finish processing {}", index);
            handShakeForNextPhase();
        }

        public void waitForDoingCommitOrRollback()
        {
            setPhase(ConsumerPhase.WaitForCommitOrRollback);
            handShakeForNextPhase();
        }

        public void finishResolution()
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Finish resolution {}", index);
            doCommit = false;
            handShakeForNextPhase();
        }
    }
}
