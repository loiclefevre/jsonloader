package com.oracle.jsonloader.exception;

public class SODACollectionMetadataUnmarshallException extends JSONLoaderException {
    public SODACollectionMetadataUnmarshallException(Throwable t) {
        super(ErrorCode.SODACollectionMetadataUnmarshall,"Failed to unmarshall SODA Collection Metadata",t);
    }
}
