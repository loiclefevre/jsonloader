package com.oracle.jsonloader.command.mongodbbsontoajdosonloading;

public interface BlockingQueueCallback {
    void addConsumed(long number, long storedSize, boolean finished);
    void addProduced(long number, long readSize, boolean finished);
}
