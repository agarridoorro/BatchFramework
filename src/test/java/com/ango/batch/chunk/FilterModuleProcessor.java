package com.ango.batch.chunk;

import com.ango.batch.IProcessor;

public class FilterModuleProcessor implements IProcessor<Integer, String>
{
    private int module;
    private int idx = 0;
    private int processToFail;

    public FilterModuleProcessor()
    {
        this.module = -1;
        this.processToFail = -1;
    }

    public FilterModuleProcessor setProcessToFail(int value)
    {
        this.processToFail = value;
        return this;
    }

    public FilterModuleProcessor setFilterModule(int value)
    {
        this.module = value;
        return this;
    }

    @Override
    public String process(Integer item)
    {
        if (++idx == processToFail)
        {
            throw new RuntimeException("Error processing element [" + item + "]");
        }

        if (null == item || (module > 0 && item % module == 0))
        {
            return null;
        }
        return item.toString();
    }

}
