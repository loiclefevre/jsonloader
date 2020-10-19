package com.oracle.jsonloader.exception;

public class JSONLoaderException extends Exception {
    protected final ErrorCode errorCode;
    protected Throwable throwable;

    protected JSONLoaderException(final ErrorCode errorCode, final String message) {
        super(message);
        this.errorCode = errorCode;
    }

    protected JSONLoaderException(final ErrorCode errorCode, final String message, final Throwable throwable) {
        super(message);
        this.errorCode = errorCode;
        this.throwable = throwable;
    }

    public void displayMessageAndExit(String lastMessage) {
        System.err.println(getMessage());
        if(throwable != null) {
            throwable.printStackTrace(System.err);
        }
        System.err.flush();
        System.out.println(lastMessage);
        System.exit(errorCode.internalErrorCode);
    }
}
