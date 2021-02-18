package com.oracle.jsonloader.util;

import java.io.File;
import java.io.FilenameFilter;
import java.util.regex.Pattern;

public class JSONDataFilenameFilter implements FilenameFilter {
    private final Pattern pattern;

    public JSONDataFilenameFilter() {
        pattern = Pattern.compile("(.+)\\.json(\\.gz)?", Pattern.CASE_INSENSITIVE);
    }

    @Override
    public boolean accept(File dir, String name) {
        return pattern.matcher(name).matches();
    }
}
