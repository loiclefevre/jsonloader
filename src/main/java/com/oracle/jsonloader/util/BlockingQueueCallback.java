package com.oracle.jsonloader.util;

public interface BlockingQueueCallback {
    void setFileNameLoaded(String fileNameLoaded);
    void addConsumed(long number, long storedSize, boolean finished);
    void addProduced(long number, long readSize, boolean finished);
}
