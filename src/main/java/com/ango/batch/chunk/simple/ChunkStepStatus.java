package com.ango.batch.chunk.simple;

import com.ango.batch.status.CommonStatus;

public class ChunkStepStatus extends CommonStatus
{
    private long initChunkTime = 0;
    private long elapsedChunkTime = 0;
    private int read = 0;
    private int skipped = 0;
    private int written = 0;
    private int lastWritten = 0;
    private int lastSkiped = 0;
    private boolean commitPending = false;
    private int committed = 0;
    private boolean isConsolidated = true;

    public ChunkStepStatus(String name)
    {
        super(name);
    }

    @Override
    public void reset()
    {
        super.reset();
        initChunkTime = 0;
        elapsedChunkTime = 0;
        read = 0;
        skipped = 0;
        written = 0;
        lastWritten = 0;
        lastSkiped = 0;
        committed = 0;
        commitPending = false;
        isConsolidated = true;
    }

    public void startChunk()
    {
        initChunkTime = System.currentTimeMillis();
    }

    public void stopChunk() { stopChunk(System.currentTimeMillis()); }

    private void stopChunk(long endChunkTime)
    {
        if (initChunkTime > 0)
        {
            elapsedChunkTime = endChunkTime - initChunkTime;
            initChunkTime = 0;
        }
    }

    @Override
    public void stop(Throwable e)
    {
        stopChunk(endTime());
        super.stop(e);
    }

    public void read(int size)
    {
        read += size;
    }

    public void skip(int size)
    {
        if (size > 0)
        {
            skipped += size;
            lastSkiped = size;
            isConsolidated = false;
        }
    }

    public void write(int size)
    {
        if (size > 0)
        {
            written += size;
            lastWritten = size;
            isConsolidated = false;
        }
    }

    public void commit()
    {
        committed++;
        commitPending = true;
        isConsolidated = false;
    }

    public void undo()
    {
        if (!isConsolidated)
        {
            written -= lastWritten;
            skipped -= lastSkiped;
            if (commitPending) committed--;
            lastWritten = 0;
            lastSkiped = 0;
            isConsolidated = true;
        }
    }

    public void consolidate()
    {
        lastWritten = 0;
        lastSkiped = 0;
        commitPending = false;
        isConsolidated = true;
    }

    @Override
    public int read()
    {
        return read;
    }

    @Override
    public int skipped()
    {
        return skipped;
    }

    @Override
    public int written()
    {
        return written;
    }

    @Override
    public long lastElapsed()
    {
        return elapsedChunkTime;
    }

    @Override
    public int committed()
    {
        return committed;
    }
}
