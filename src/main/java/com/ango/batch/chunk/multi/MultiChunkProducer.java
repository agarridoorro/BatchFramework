package com.ango.batch.chunk.multi;

import com.ango.batch.resource.ReaderResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class MultiChunkProducer<T> implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiChunkProducer.class);

    private final ProducerData<T> data;
    private final ReaderResource<T> reader;

    public MultiChunkProducer(ReaderResource<T> reader, ProducerData<T> data)
    {
        this.reader = reader;
        this.data = data;
    }

    @Override
    public void run()
    {
        final int maxItemsToRead = data.producerActions().consumersCount() * data.producerActions().commitInterval();

        ///***************************
        ///     WAIT FOR QUERY DATA
        ///***************************
        while (data.producerActions().waitForQueryData())
        {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("reading data");
            ///***************************
            ///     READING DATA
            ///***************************
            try
            {
                final List<T> readItems = reader.read(maxItemsToRead);
                putData(readItems);
            }
            catch (Throwable t)
            {
                LOGGER.error("Error in consumer", t);
                data.producerActions().add(t);
            }

            ///***************************
            ///     DATA READY
            ///***************************
            data.producerActions().dataReady();
        }
        if (LOGGER.isDebugEnabled()) LOGGER.debug("Finished");
    }

    private void putData(List<T> items)
    {
        final int totalSize = items.size();
        data.status().read(totalSize);
        data.producerActions().totalRead(totalSize);
        int globalIndex = 0;
        for (int idxConsumer = 0; idxConsumer < data.producerActions().consumersCount(); idxConsumer++)
        {
            data.producerActions().clear(idxConsumer);
            for (int idxItem = 0; idxItem < data.producerActions().commitInterval(); idxItem++)
            {
                if (globalIndex == totalSize)
                {
                    break;
                }
                final T item = items.get(globalIndex++);
                data.producerActions().addData(idxConsumer, item);
            }
        }
    }

}
