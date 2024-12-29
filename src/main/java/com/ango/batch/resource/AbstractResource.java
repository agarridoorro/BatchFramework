package com.ango.batch.resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ango.batch.IResource;
import com.ango.batch.status.IExceptionVault;

import java.io.IOException;

public abstract class AbstractResource implements IResource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractResource.class);

    private boolean isOpen;
    private final IResource resource;

    public AbstractResource(Object res)
    {
        isOpen = false;
        if (res instanceof IResource)
        {
            resource = (IResource) res;
        }
        else
        {
            resource = null;
        }
    }

    @Override
    public void open() throws IOException
    {
        if (null != resource && !isOpen)
        {
            resource.open();
            isOpen = true;
        }
    }

    @Override
    public void close() throws IOException
    {
        if (null != resource && isOpen)
        {
            resource.close();
            isOpen = false;
        }
    }

    public void tryClose(IExceptionVault vault)
    {
        try
        {
            close();
        }
        catch (Throwable t)
        {
            LOGGER.error("Error closing resource [" + resource.getClass() + "]", t);
            vault.add(t);
        }
    }
}
