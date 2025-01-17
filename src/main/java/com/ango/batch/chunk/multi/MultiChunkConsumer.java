package com.ango.batch.chunk.multi;

import com.ango.batch.resource.ProcessorResource;
import com.ango.batch.resource.WriterResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.TransactionManager;
import java.util.List;

class MultiChunkConsumer<T,K> implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiChunkConsumer.class);

    private final TransactionManager tm;
    private final ConsumerData<T> data;
    private final ProcessorResource<T,K> processor;
    private final WriterResource<K> writer;

    public MultiChunkConsumer(TransactionManager tm, ProcessorResource<T, K> processor,
                              WriterResource<K> writer, ConsumerData<T> data)
    {
        this.tm = tm;
        this.data = data;
        this.processor = processor;
        this.writer = writer;
    }

    @Override
    public void run()
    {
        ///***************************
        ///     WAIT FOR DATA
        ///***************************
        while (data.consumerActions().waitForData())
        {
            ///***************************
            ///     PROCESS
            ///***************************
            data.consumerActions().setPhase(ConsumerPhase.Preprocess);

            final List<T> readItems = data.consumerActions().getItems();
            final int read = readItems.size();
            int skipped = 0;
            int written = 0;

            boolean isTransactionPending = false;
            try
            {
                tm.begin();
                isTransactionPending = true;
                if (read > 0)
                {
                    try
                    {
                        data.consumerActions().setPhase(ConsumerPhase.Process);
                        final List<K> itemsProcessed = processor.process(readItems);
                        written = itemsProcessed.size();
                        skipped = read - written;
                        if (written > 0)
                        {
                            writer.write(itemsProcessed);
                        }
                    }
                    catch (Throwable t)
                    {
                        LOGGER.error("Error in thread: [{}] phase: [{}]", data.consumerActions().index(), data.consumerActions().phase(), t);
                        data.consumerActions().add(t);
                    }
                }
            }
            catch (Throwable t)
            {
                LOGGER.error("Error in thread: [{}] phase: [{}]", data.consumerActions().index(), data.consumerActions().phase(), t);
                data.consumerActions().add(t);
            }

            ///***************************
            ///     FINISH PROCESSING
            ///***************************
            data.consumerActions().finishProcessing();

            ///***************************
            ///     WAIT FOR COMMIT OR ROLLBACK
            ///***************************
            data.consumerActions().waitForDoingCommitOrRollback();

            try
            {
                ///***************************
                ///     DO COMMIT
                ///***************************
                if (data.consumerActions().doCommit())
                {
                    data.consumerActions().setPhase(ConsumerPhase.Commit);
                    //Something went wrong, de transaction was rolled back
                    if (!isTransactionPending)
                    {
                        LOGGER.error("Error in thread: [{}] phase: [{}] The transaction was already rolled back", data.consumerActions().index(), data.consumerActions().phase());
                        data.consumerActions().add(new Exception("The transaction was already rolled back"));
                    }
                    else
                    {
                        data.status().skip(skipped);
                        data.status().write(written);
                        data.status().commit();
                        if (data.consumerActions().stopChunk())
                        {
                            data.status().stopChunk();
                        }
                        data.status().save();
                        tm.commit();
                        data.status().consolidate();
                    }
                }
                ///***************************
                ///     DO ROLLBACK
                ///***************************
                else
                {
                    data.consumerActions().setPhase(ConsumerPhase.Rollback);
                    if (isTransactionPending)
                    {
                        tm.rollback();
                    }
                }
            }
            catch (Throwable t)
            {
                LOGGER.error("Error in thread: [{}] phase: [{}]", data.consumerActions().index(), data.consumerActions().phase());
                data.consumerActions().add(t);
                data.status().undo();
                //If a commit was failed, try to do rollback
                if (data.consumerActions().doCommit() && isTransactionPending)
                {
                    try
                    {
                        tm.rollback();
                    }
                    catch (Throwable t2)
                    {
                        LOGGER.error("Error in thread: [{}] phase: [{}]", data.consumerActions().index(), data.consumerActions().phase());
                        data.consumerActions().add(t2);
                    }
                }
            }

            ///***************************
            ///     FINISH RESOLUTION
            ///***************************
            data.consumerActions().finishResolution();
        }

        data.consumerActions().setPhase(ConsumerPhase.Finished);
    }
}
