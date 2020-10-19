package com.oracle.jsonloader.exception;

public class SourceDirectoryNotFoundException extends JSONLoaderException {
    public SourceDirectoryNotFoundException(String path) {
        super(ErrorCode.SourceDirectoryNotFound,String.format("Source directory for files to migrate not found!", path));
    }
}
