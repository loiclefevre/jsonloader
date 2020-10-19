package com.oracle.jsonloader.model;

public class IndexColumn {
    public final String name;
    public final boolean asc;

    public IndexColumn(String name, boolean asc) {
        this.name = name;
        this.asc = asc;
    }
}
