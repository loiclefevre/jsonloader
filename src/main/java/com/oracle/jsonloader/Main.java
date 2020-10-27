package com.oracle.jsonloader;

import com.oracle.jsonloader.command.mongodbbsontoajdosonloading.MongoDBBSONToAJDOSONLoading;
import com.oracle.jsonloader.exception.BadCLIUsageException;
import com.oracle.jsonloader.exception.JSONLoaderException;
import com.oracle.jsonloader.util.Console;

import static com.oracle.jsonloader.util.Console.*;

public class Main {

    public static final String VERSION = "1.0.1";

    enum Command {
        MongoDBBSONToOSONLoading
    }

    public static void main(final String[] args) {
        final long totalDuration = System.currentTimeMillis();

        try {
            banner();

            switch(getCommand(args)) {
                case MongoDBBSONToOSONLoading:
                    MongoDBBSONToAJDOSONLoading.main(args);
                    break;
            }
        }
        catch(JSONLoaderException e) {
            e.displayMessageAndExit(Style.ANSI_BLUE + "duration: " + getDurationSince(totalDuration) + Style.ANSI_RESET);
        }
        finally {
            println(Console.Style.ANSI_BLUE + "duration: " + getDurationSince(totalDuration));
        }
    }

    private static void banner() {
        print(String.format("%sJSONloader v%s", Style.ANSI_YELLOW, VERSION));
        println();
        println();
    }

    private static Command getCommand(String[] args) throws JSONLoaderException {
        if(args.length == 0) {
            throw new BadCLIUsageException();
        }

        switch(args[0].toLowerCase()) {
            case "1":
                return Command.MongoDBBSONToOSONLoading;

            default:
                throw new BadCLIUsageException();
        }
    }
}
