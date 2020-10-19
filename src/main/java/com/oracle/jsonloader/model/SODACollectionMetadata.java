package com.oracle.jsonloader.model;

public class SODACollectionMetadata {
    private SODACollectionKeyColumnMetadata keyColumn;

    public SODACollectionMetadata() {
    }

    public SODACollectionKeyColumnMetadata getKeyColumn() {
        return keyColumn;
    }

    public void setKeyColumn(SODACollectionKeyColumnMetadata keyColumn) {
        this.keyColumn = keyColumn;
    }
}
