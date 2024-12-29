package com.ango.batch.chunk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ango.batch.IReader;
import com.ango.batch.IResource;

public class GenerateNumbersReader implements IReader<Integer>, IResource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateNumbersReader.class);

    private int readsToFail;
    private int max = 0;
    private int idx = 0;

    public GenerateNumbersReader()
    {
        this.readsToFail = -1;
        this.max = -1;
    }

    public GenerateNumbersReader setReadsToFail(int value)
    {
        this.readsToFail = value;
        return this;
    }

    public GenerateNumbersReader setMax(int value)
    {
        this.max = value;
        return this;
    }

    @Override
    public Integer read()
    {
        idx++;
        if (idx == readsToFail)
        {
            throw new RuntimeException("Error reading element [" + idx + "]");
        }
        return (idx > max) ? null : idx;
    }

    @Override
    public void open()
    {
        LOGGER.debug("Opening GenerateNumbersReader");
    }

    @Override
    public void close()
    {
        LOGGER.debug("Closing GenerateNumbersReader");
    }
}
