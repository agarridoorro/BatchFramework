package com.ango.batch.resource;

import com.ango.batch.IReader;

import java.util.LinkedList;
import java.util.List;

public class ReaderResource<T> extends AbstractResource implements IReader<T>
{
    private final IReader<T> reader;

    public ReaderResource(IReader<T> reader)
    {
        super(reader);
        this.reader = reader;
    }

    @Override
    public T read()
    {
        return reader.read();
    }

	public List<T> read(int commitInterval)
	{
		final List<T> items = new LinkedList<>();
		for (int i = 0; i < commitInterval; i++)
		{
			final T item = reader.read();
			if (null == item)
			{
				break;
			}
			items.add(item);
		}
		return items;
	}
}
