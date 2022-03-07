package io.tapdata.pdk.cli;

import picocli.CommandLine;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 *
 * @author aplomb
 */
public class DiscoverSchemaMain {
    //
    public static void main(String... args) {
        args = new String[] {"discoverSchema",
                "--id", "vika-pdk",
                "--group", "tapdata",
                "--buildNumber", "1",
                "--connectionConfig", "{'token' : 'uskMiSCZAbukcGsqOfRqjZZ', 'spaceId' : 'spcvyGLrtcYgs'}"};

        Main.registerCommands().parseWithHandler(new CommandLine.RunLast(), args);
    }
}