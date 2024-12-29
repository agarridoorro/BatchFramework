package com.ango.batch.resource;

import com.ango.batch.IProcessor;

import java.util.LinkedList;
import java.util.List;

public class ProcessorResource<T,K> extends AbstractResource implements IProcessor<T,K>
{
    private final IProcessor<T,K> processor;

    public ProcessorResource(IProcessor<T,K> processor)
    {
        super(processor);
        this.processor = processor;
    }

    @SuppressWarnings("unchecked")
    @Override
    public K process(T item)
    {
        return (null == processor) ? (K) item : processor.process(item);
    }

    @SuppressWarnings("unchecked")
	public List<K> process(List<T> itemsIn)
	{
        if (null == processor)
        {
            return (List<K>) itemsIn;
        }

		final List<K> itemsOut = new LinkedList<>();
		for (final T itemIn : itemsIn)
		{
            final K itemOut = processor.process(itemIn);
            if (null != itemOut)
            {
                itemsOut.add(itemOut);
            }
		}
		return itemsOut;
	}
}
