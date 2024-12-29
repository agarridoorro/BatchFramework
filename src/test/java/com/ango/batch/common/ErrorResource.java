package com.ango.batch.common;

import com.ango.batch.IResource;

import java.io.IOException;

public class ErrorResource implements IResource
{
    private final boolean failOnOpen;
    private final boolean isFailOnClose;

    public ErrorResource()
    {
        this.failOnOpen = false;
        this.isFailOnClose = false;
    }

    public ErrorResource(boolean failOnOpen, boolean isFailOnClose)
    {
        this.failOnOpen = failOnOpen;
        this.isFailOnClose = isFailOnClose;
    }

    @Override
    public void open() throws IOException
    {
        if (failOnOpen)
        {
            throw new IOException("Error opening tasklet");
        }
    }

    @Override
    public void close() throws IOException
    {
        if (isFailOnClose)
        {
            throw new IOException("Error closing tasklet");
        }
    }
}
