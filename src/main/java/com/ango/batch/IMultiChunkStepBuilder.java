package com.ango.batch;

import com.ango.batch.chunk.multi.MultiChunkStatus;
import com.ango.batch.chunk.multi.MultiChunkStep;
import com.ango.batch.exceptions.ValidationException;
import com.ango.batch.tx.BatchTransactionManager;
import org.apache.commons.lang3.StringUtils;

import javax.transaction.TransactionManager;
import java.util.ArrayList;
import java.util.List;

public interface IMultiChunkStepBuilder<T,K> extends IStepBuilder<IMultiChunkStepBuilder<T,K>>
{
    IMultiChunkStepBuilder<T,K> setReader(IReader<T> reader);

    IMultiChunkStepBuilder<T,K> addProcessor(IProcessor<T,K> processor);

    IMultiChunkStepBuilder<T,K> addWriter(IWriter<K> writer);

    IMultiChunkStepBuilder<T,K> setCommitInterval(int size);

    IMultiChunkStepBuilder<T,K> setConsumers(int consumers);

    IMultiChunkStepBuilder<T,K> setWaitTimeout(int timeout);

    IMultiChunkStepBuilder<T,K> setTransactionManager(TransactionManager transactionManager);

    static <T,K> IMultiChunkStepBuilder<T,K> instance()
    {
        return new IMultiChunkStepBuilder<>()
        {
            private String name = null;
            private IReader<T> reader = null;
            private final List<IProcessor<T,K>> processors = new ArrayList<>();
            private final List<IWriter<K>> writers = new ArrayList<>();
            private int commitInterval = 1;
            private int consumers;
            private int waitTimeout;
            private boolean throwExceptions = true;
            private TransactionManager transactionManager;

            @Override
            public IMultiChunkStepBuilder<T,K> setName(String name)
            {
                this.name = name;
                return this;
            }

            @Override
            public IMultiChunkStepBuilder<T,K> setReader(IReader<T> reader)
            {
                this.reader = reader;
                return this;
            }

            @Override
            public IMultiChunkStepBuilder<T,K> addProcessor(IProcessor<T, K> processor)
            {
                this.processors.add(processor);
                return this;
            }

            @Override
            public IMultiChunkStepBuilder<T,K> addWriter(IWriter<K> writer)
            {
                this.writers.add(writer);
                return this;
            }

            @Override
            public IMultiChunkStepBuilder<T,K> setCommitInterval(int size)
            {
                this.commitInterval = size;
                return this;
            }

            @Override
            public IMultiChunkStepBuilder<T, K> setConsumers(int consumers)
            {
                this.consumers = consumers;
                return this;
            }

            @Override
            public IMultiChunkStepBuilder<T, K> setWaitTimeout(int timeout)
            {
                this.waitTimeout = timeout;
                return this;
            }

            @Override
            public IMultiChunkStepBuilder<T, K> setTransactionManager(TransactionManager transactionManager)
            {
                this.transactionManager = transactionManager;
                return this;
            }

            @Override
            public IMultiChunkStepBuilder<T,K> setThrowExceptions(boolean value)
            {
                this.throwExceptions = value;
                return this;
            }

            @Override
            public IStep build()
            {
                validate();
                final MultiChunkStep<T,K> step = new MultiChunkStep<>();
                step.setStatus(new MultiChunkStatus(name));
                step.setReader(reader);
                for (final IProcessor<T,K> processor : processors)
                {
                    step.addProcessor(processor);
                }
                for (final IWriter<K> writer : writers)
                {
                    step.addWriter(writer);
                }
                step.setCommitInterval(commitInterval);
                step.setThrowExceptions(throwExceptions);
                step.setTransactionManager(null == transactionManager ? BatchTransactionManager.getInstance() : transactionManager);
                step.setConsumers(consumers);
                step.setWaitTimeout(waitTimeout);
                return step;
            }

            private void validate()
            {
                if (StringUtils.isBlank(name))
                {
                    throw new ValidationException("The step must have a name");
                }
                if (null == reader)
                {
                    throw new ValidationException("The reader cannot be null");
                }
                if (consumers <= 0)
                {
                    throw new ValidationException("The threads must be positive");
                }
                if (writers.size() != consumers)
                {
                    throw new ValidationException("The writers must be the same number as threads");
                }
                if (!processors.isEmpty())
                {
                    if (processors.size() != consumers)
                    {
                        throw new ValidationException("The processors must be 0 or the same number as threads");
                    }
                }
                if (waitTimeout < 0)
                {
                    throw new ValidationException("The timeout for consumers must be greater or equals to zero");
                }
            }
        };
    }
}
