package com.oracle.jsonloader.exception;

public class SODACollectionCantKeepMongoDBIdException extends JSONLoaderException {
    public SODACollectionCantKeepMongoDBIdException(String collectionName) {
        super(ErrorCode.SODACollectionCantKeepMongoDBId,String.format("The collection %s already exists but is not configured to allow to keep existing MongoDB ObjectIds!", collectionName));
    }
}
