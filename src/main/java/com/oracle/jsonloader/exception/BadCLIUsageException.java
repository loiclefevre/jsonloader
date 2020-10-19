package com.oracle.jsonloader.exception;

import com.oracle.jsonloader.Main;

public class BadCLIUsageException extends JSONLoaderException {
    public BadCLIUsageException() {
        super(ErrorCode.BadCLIUsage,
                "Usage: -Xmx4G -Xms4G -jar jsonloader-"+ Main.VERSION+"-jar-with-dependencies.jar <command> <command specific parameters>\n\n" +
                        "Where command is:\n" +
                        "- 1: for loading MongoDB dumped collection(s) data into Autonomous JSON Database\n" +
                        "\t. parameter #1: dump directory where source files could be found\n" +
                        "\t. parameter #2: source database name (to find source files)\n" +
                        "\t. parameter #3: destination database service name (to load JSON documents into)\n" +
                        "\t. parameter #4: destination database user name to connect to\n" +
                        "\t. parameter #5: destination database password to connect to\n" +
                        "\t. parameter #6: (optional) batch size to use (default: 5,000)\n" +
                        "\t. parameter #7: (optional) true to keep the auto-generated MongoDB ObjectIds during import (default: true)\n" +
                        "\n\n");
    }
}
