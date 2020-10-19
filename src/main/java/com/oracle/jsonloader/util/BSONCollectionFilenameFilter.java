package com.oracle.jsonloader.util;

import java.io.File;
import java.io.FilenameFilter;
import java.util.regex.Pattern;

public class BSONCollectionFilenameFilter implements FilenameFilter {
    private final Pattern pattern;

    public BSONCollectionFilenameFilter(String collectionName) {
        pattern = Pattern.compile(collectionName + "(_\\d+)?\\.bson(\\.gz)?", Pattern.CASE_INSENSITIVE);
    }

    @Override
    public boolean accept(File dir, String name) {
        return pattern.matcher(name).matches();
    }
}
