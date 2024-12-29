package com.ango.batch.tx;

import com.atomikos.icatch.jta.UserTransactionImp;

import javax.transaction.*;

public class BatchUserTransaction implements UserTransaction
{
    private UserTransactionImp inner;
    private  BatchUserTransaction() {}

    public static UserTransaction getInstance()
    {
        return new BatchUserTransaction();
    }

    @Override
    public void begin() throws NotSupportedException, SystemException
    {

    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException
    {

    }

    @Override
    public void rollback() throws IllegalStateException, SecurityException, SystemException
    {

    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException
    {

    }

    @Override
    public int getStatus() throws SystemException
    {
        return 0;
    }

    @Override
    public void setTransactionTimeout(int i) throws SystemException
    {

    }
}
