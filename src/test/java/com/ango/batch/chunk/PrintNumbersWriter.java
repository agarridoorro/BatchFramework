package com.ango.batch.chunk;

import com.ango.batch.IWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

public class PrintNumbersWriter implements IWriter<String>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PrintNumbersWriter.class);

    private int writesToFail;
    private int maxMillisToWrite;
    private int minDuration;
    private int idx = 0;

    public PrintNumbersWriter()
    {
        this.writesToFail = -1;
        this.maxMillisToWrite = -1;
        this.minDuration = -1;
    }

    public PrintNumbersWriter setWritesToFail(int value)
    {
        this.writesToFail = value;
        return this;
    }

    public PrintNumbersWriter setMaxMillisToWrite(int value)
    {
        this.maxMillisToWrite = value;
        return this;
    }

    public PrintNumbersWriter setMinDuration(int value)
    {
        this.minDuration = value;
        return this;
    }

    @Override
    public void write(List<String> items)
    {
        if (minDuration > 0)
        {
            try
            {
                Thread.sleep(minDuration);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
        Random random = new Random();
        for (String item : items)
        {
            if (maxMillisToWrite > 0)
            {
                try
                {
                    Thread.sleep(random.nextInt(maxMillisToWrite + 1));
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }

            if (++idx == writesToFail)
            {
                throw new RuntimeException("Error writing element [" + item + "]");
            }

            LOGGER.info("Writing number {}", item);
            //System.out.println(item);
        }
    }

}
