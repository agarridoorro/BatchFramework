package com.ango.batch;

import com.ango.batch.tx.BatchTransactionManager;
import org.apache.commons.lang3.StringUtils;

import com.ango.batch.chunk.simple.ChunkStep;
import com.ango.batch.chunk.simple.ChunkStepStatus;
import com.ango.batch.exceptions.ValidationException;

import javax.transaction.TransactionManager;

public interface IChunkStepBuilder<T,K> extends IStepBuilder<IChunkStepBuilder<T,K>>
{
    IChunkStepBuilder<T,K> setReader(IReader<T> reader);

    IChunkStepBuilder<T,K> setProcessor(IProcessor<T,K> processor);

    IChunkStepBuilder<T,K> setWriter(IWriter<K> writer);

    IChunkStepBuilder<T,K> setCommitInterval(int size);

    IChunkStepBuilder<T,K> setTransactionManager(TransactionManager transactionManager);

    static <T,K> IChunkStepBuilder<T,K> instance()
    {
        return new IChunkStepBuilder<>()
        {
            private String name = null;
            private IReader<T> reader = null;
            private IProcessor<T,K> processor = null;
            private IWriter<K> writer = null;
            private int commitInterval = 1;
            private boolean throwExceptions = true;
            private TransactionManager transactionManager;

            @Override
            public IChunkStepBuilder<T,K> setName(String name)
            {
                this.name = name;
                return this;
            }

            @Override
            public IChunkStepBuilder<T,K> setReader(IReader<T> reader)
            {
                this.reader = reader;
                return this;
            }

            @Override
            public IChunkStepBuilder<T,K> setProcessor(IProcessor<T, K> processor)
            {
                this.processor = processor;
                return this;
            }

            @Override
            public IChunkStepBuilder<T,K> setWriter(IWriter<K> writer)
            {
                this.writer = writer;
                return this;
            }

            @Override
            public IChunkStepBuilder<T,K> setCommitInterval(int size)
            {
                this.commitInterval = size;
                return this;
            }

            @Override
            public IChunkStepBuilder<T, K> setTransactionManager(TransactionManager transactionManager)
            {
                this.transactionManager = transactionManager;
                return this;
            }

            @Override
            public IChunkStepBuilder<T,K> setThrowExceptions(boolean value)
            {
                this.throwExceptions = value;
                return this;
            }

            @Override
            public IStep build()
            {
                validate();
                final ChunkStep<T,K> step = new ChunkStep<>();
                step.setStatus(new ChunkStepStatus(name));
                step.setReader(reader);
                step.setProcessor(processor);
                step.setWriter(writer);
                step.setCommitInterval(commitInterval);
                step.setThrowExceptions(throwExceptions);
                step.setTransactionManager(null == transactionManager ? BatchTransactionManager.getInstance() : transactionManager);
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
                if (null == writer)
                {
                    throw new ValidationException("The writer cannot be null");
                }
            }
        };
    }
}
