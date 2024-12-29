package com.ango.batch.chunk.multi;

public enum ConsumerPhase
{
    WaitForData("Waiting for data"),
    Preprocess("Pre-processing"),
    Process("Processing"),
    WaitForCommitOrRollback("Waiting for commit or rollback"),
    Commit("Committing"),
    Rollback("Rolling back"),
    WaitForFinish("Waiting for finish"),
    Finished("Finished");

    private final String description;

    ConsumerPhase(String description)
    {
        this.description = description;
    }

    @Override
    public String toString()
    {
        return description;
    }
}

