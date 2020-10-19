package com.oracle.jsonloader.exception;

public class UnknownException extends JSONLoaderException {
    public UnknownException(ErrorCode errorCode, String message, Throwable throwable) {
        super(errorCode, message, throwable);
    }
}
