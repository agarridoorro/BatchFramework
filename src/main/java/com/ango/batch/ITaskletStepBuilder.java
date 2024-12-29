package com.ango.batch;

import com.ango.batch.tx.BatchTransactionManager;
import org.apache.commons.lang3.StringUtils;

import com.ango.batch.exceptions.ValidationException;
import com.ango.batch.tasklet.TaskletStep;
import com.ango.batch.tasklet.TaskletStepStatus;

import javax.transaction.TransactionManager;

public interface ITaskletStepBuilder extends IStepBuilder<ITaskletStepBuilder>
{
    ITaskletStepBuilder setTasklet(ITasklet tasklet);

    ITaskletStepBuilder setTransactional(boolean value);

    ITaskletStepBuilder setTransactionManager(TransactionManager transactionManager);

    static ITaskletStepBuilder instance()
    {
        return new ITaskletStepBuilder()
        {
            private String name = null;
            private ITasklet tasklet = null;
            private boolean throwExceptions = false;
            private boolean isTransactional = true;
            private TransactionManager transactionManager;

            @Override
            public ITaskletStepBuilder setName(String name)
            {
                this.name = name;
                return this;
            }

            @Override
            public ITaskletStepBuilder setTasklet(ITasklet tasklet)
            {
                this.tasklet = tasklet;
                return this;
            }

            @Override
            public ITaskletStepBuilder setThrowExceptions(boolean value)
            {
                this.throwExceptions = value;
                return this;
            }

            @Override
            public ITaskletStepBuilder setTransactional(boolean value)
            {
                this.isTransactional = value;
                return this;
            }

            @Override
            public ITaskletStepBuilder setTransactionManager(TransactionManager transactionManager)
            {
                this.transactionManager = transactionManager;
                return this;
            }

            @Override
            public IStep build()
            {
                validate();
                final TaskletStep step = new TaskletStep();
                step.setStatus(new TaskletStepStatus(name));
                step.setTasklet(tasklet);
                step.setThrowExceptions(throwExceptions);
                step.setTransactional(isTransactional);
                if (isTransactional)
                {
                    step.setTransactionManager(null == transactionManager ? BatchTransactionManager.getInstance() : transactionManager);
                }
                return step;
            }

            private void validate()
            {
                if (StringUtils.isBlank(name))
                {
                    throw new ValidationException("The step must have a name");
                }
                if (null == tasklet)
                {
                    throw new ValidationException("The tasklet cannot be null");
                }
                if (!isTransactional && transactionManager != null)
                {
                    throw new ValidationException("The tasklet is not transactional");
                }
            }
        };
    }
}
