package io.tapdata.pdk.cli;

import picocli.CommandLine;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 *
 * @author aplomb
 */
public class BootMain {
    public static void main(String... args) {
        args = new String[]{"template", "-g", "xyz.dexter", "-n", "ProjectTest", "-v", "0.0.1",
                "-o", "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/connectors"
        };

        Main.registerCommands().parseWithHandler(new CommandLine.RunLast(), args);
    }
}
