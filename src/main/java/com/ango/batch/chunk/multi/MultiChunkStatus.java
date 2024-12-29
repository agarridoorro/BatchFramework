package com.ango.batch.chunk.multi;

import com.ango.batch.chunk.simple.ChunkStepStatus;
import com.ango.batch.exceptions.ConsumerException;

public class MultiChunkStatus extends ChunkStepStatus
{
    public MultiChunkStatus(String name)
    {
        super(name);
    }

    public void add(int index, ConsumerPhase phase, Throwable t)
    {
        stop(new ConsumerException(index, phase, t));
    }
}
