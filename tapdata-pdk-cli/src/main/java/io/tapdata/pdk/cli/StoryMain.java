package io.tapdata.pdk.cli;

import io.tapdata.pdk.cli.commands.*;
import picocli.CommandLine;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 *
 * @author aplomb
 */
public class StoryMain {
    //
    public static void main(String... args) {
        String rootPath = "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/tapdata-pdk-cli/src/main/resources/stories/";
        args = new String[]{"start",
                rootPath + "emptyToFile.json",
//                rootPath + "vikaToVika.json",
        };

        Main.registerCommands().parseWithHandler(new CommandLine.RunLast(), args);
    }

}
