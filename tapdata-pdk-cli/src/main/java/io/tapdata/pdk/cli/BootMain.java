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
        args = new String[]{"boot", "-g", "xyz.dexter", "-n", "DexterTest", "-v", "0.0.1", "-o", "/Users/dxtr/TapData/tapdata-sdk/connectors"};

        Main.registerCommands().parseWithHandler(new CommandLine.RunLast(), args);
    }
}
