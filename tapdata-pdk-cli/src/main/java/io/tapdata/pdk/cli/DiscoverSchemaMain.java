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
        args = new String[] {"discoverSchema", "-i", "vika-pdk", "-g", "tapdata", "-b", "1", "-c", "{'token' : 'uskMiSCZAbukcGsqOfRqjZZ', 'spaceId' : 'spcvyGLrtcYgs'}"};

        Main.registerCommands().parseWithHandler(new CommandLine.RunLast(), args);
    }
}
