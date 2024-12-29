package com.ango.batch.resource;

import java.util.List;

import com.ango.batch.IWriter;

public  class WriterResource<K> extends AbstractResource implements IWriter<K>
{
    private IWriter<K> writer;

    public WriterResource(IWriter<K> writer)
    {
        super(writer);
        this.writer = writer;
    }

    @Override
    public void write(List<K> items)
    {
        writer.write(items);
    }
}
