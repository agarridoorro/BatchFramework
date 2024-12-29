package com.ango.batch.tx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.*;

public class BatchTransactionManager implements TransactionManager
{
	private static final Logger LOGGER = LoggerFactory.getLogger(BatchTransactionManager.class);


	private BatchTransactionManager() { }

	public static TransactionManager getInstance()
	{
		return new BatchTransactionManager();
	}

	@Override
	public void begin() throws NotSupportedException, SystemException
	{
		LOGGER.debug("begin");
		//tm.begin();
	}

	@Override
	public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException
	{
		LOGGER.debug("commit");
		//tm.commit();
	}

	@Override
	public int getStatus() throws SystemException
	{
		return 0;
		//return tm.getStatus();
	}

	@Override
	public Transaction getTransaction() throws SystemException
	{
		return null;
		//return tm.getTransaction();
	}

	@Override
	public void resume(Transaction transaction) throws InvalidTransactionException, IllegalStateException, SystemException
	{
		//tm.resume(transaction);
	}

	@Override
	public void rollback() throws IllegalStateException, SecurityException, SystemException
	{
		LOGGER.debug("rollback");
		//tm.rollback();
	}

	@Override
	public void setRollbackOnly() throws IllegalStateException, SystemException
	{
		//tm.setRollbackOnly();
	}

	@Override
	public void setTransactionTimeout(int i) throws SystemException
	{
		//tm.setTransactionTimeout(i);
	}

	@Override
	public Transaction suspend() throws SystemException
	{
		return null;
		//return tm.suspend();
	}
}
