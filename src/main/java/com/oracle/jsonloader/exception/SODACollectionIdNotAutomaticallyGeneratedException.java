package com.oracle.jsonloader.exception;

public class SODACollectionIdNotAutomaticallyGeneratedException extends JSONLoaderException {
    public SODACollectionIdNotAutomaticallyGeneratedException(String collectionName) {
        super(ErrorCode.SODACollectionIdNotAutomaticallyGenerated, String.format("The collection %s already exists but is not configured to have IDs automatically generated!",collectionName));
    }
}
