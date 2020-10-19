package com.oracle.jsonloader.model;

import com.oracle.jsonloader.model.IndexColumn;

import java.util.ArrayList;
import java.util.List;

public class MetadataKey {
    public boolean text;
    public final List<IndexColumn> columns = new ArrayList<>();

    public void addIndexColumn(String indexColumn, boolean asc) {
        columns.add(new IndexColumn(indexColumn, asc) );
    }
}
