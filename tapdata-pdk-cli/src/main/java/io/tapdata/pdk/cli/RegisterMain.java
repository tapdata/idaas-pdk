package io.tapdata.pdk.cli;

import io.tapdata.pdk.cli.commands.MainCli;
import io.tapdata.pdk.cli.commands.RegisterCli;
import io.tapdata.pdk.cli.commands.StartCli;
import picocli.CommandLine;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 *
 * @author aplomb
 */
public class RegisterMain {
    //
    public static void main(String... args) {
        args = new String[]{
                "register", "-a", "3324cfdf-7d3e-4792-bd32-571638d4562f", "-t", "http://localhost:3000",
//                "register", "-a", "3324cfdf-7d3e-4792-bd32-571638d4562f", "-t", "http://192.168.1.126:3004",
//                "register", "-a", "3324cfdf-7d3e-4792-bd32-571638d4562f", "-t", "http://localhost:3000",

//                "/Users/aplomb/dev/tapdata/AgentProjects/tapdata-pdk/dist/empty-connector-v1.0.0.jar",
//                "/Users/aplomb/dev/tapdata/AgentProjects/tapdata-pdk/dist/file-connector-v1.0.0.jar",
                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/dist/vika-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/AgentProjects/tapdata-pdk/dist/vika-connector-v1.0.0.jar",
        };

        Main.registerCommands().parseWithHandler(new CommandLine.RunLast(), args);
    }
}
