package com.oracle.jsonloader.model;

public class MongoDBMetadata {
    private MetadataOptions options;
    private MetadataIndex[] indexes;

    public MongoDBMetadata() {
    }

    public MetadataOptions getOptions() {
        return options;
    }

    public void setOptions(MetadataOptions options) {
        this.options = options;
    }

    public MetadataIndex[] getIndexes() {
        return indexes;
    }

    public void setIndexes(MetadataIndex[] indexes) {
        this.indexes = indexes;
    }
}
