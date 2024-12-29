package com.ango.batch.exceptions;

public class SaveStateException extends Exception
{
	private static final long serialVersionUID = 1L;

	public SaveStateException(String errorMessage)
	{
        super(errorMessage);
    }

	public SaveStateException(String errorMessage, Throwable err)
	{
	    super(errorMessage, err);
	}
}
