package com.oracle.jsonloader.exception;

public enum ErrorCode {
    BadCLIUsage(-1),
    SODACollectionMetadataUnmarshall(-2),
    SODACollectionCantKeepMongoDBId(-3),
    SODACollectionIdNotAutomaticallyGenerated(-4),
    SourceDirectoryNotFound(-5),
    Unknown(-1000)
    ;

    public final int internalErrorCode;

    ErrorCode(final int errorCode) {
        this.internalErrorCode = errorCode;
    }

}
