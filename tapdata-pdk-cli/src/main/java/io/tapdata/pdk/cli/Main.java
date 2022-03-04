package io.tapdata.pdk.cli;

import io.tapdata.pdk.cli.commands.*;
import picocli.CommandLine;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 *
 * @author aplomb
 */
public class Main {
    //
    public static void main(String... args) {
//        args = new String[]{"start",
//                "/Users/aplomb/dev/tapdata/AgentProjects/dataflowfiles/empty.json",
//                "/Users/aplomb/dev/tapdata/AgentProjects/dataflowfiles/mongo.json",
//                "/Users/aplomb/dev/tapdata/AgentProjects/dataflowfiles/mongoToVika.json",
//                "/Users/aplomb/dev/tapdata/AgentProjects/dataflowfiles/mongoSimple.json",
//                "/Users/aplomb/dev/tapdata/AgentProjects/tapdata-pdk/tapdata-pdk-cli/src/main/resources/stories/vikaToVika.json",
//        };

        Main.registerCommands().parseWithHandler(new CommandLine.RunLast(), args);
    }

    public static CommandLine registerCommands() {
        CommandLine commandLine = new CommandLine(new MainCli());
        commandLine.addSubcommand("register", new RegisterCli());
        commandLine.addSubcommand("start", new StartCli());
        commandLine.addSubcommand("allTables", new AllTablesCli());
        commandLine.addSubcommand("connectionTest", new ConnectionTestCli());
        commandLine.addSubcommand("boot", new ConnectorProjectBootCli());
        return commandLine;
    }
}
