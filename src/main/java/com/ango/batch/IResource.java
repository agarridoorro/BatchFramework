package com.ango.batch;

import java.io.IOException;

public interface IResource
{
    void open() throws IOException;

    void close() throws IOException;
}
