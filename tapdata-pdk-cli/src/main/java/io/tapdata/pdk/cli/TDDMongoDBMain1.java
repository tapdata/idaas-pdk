package io.tapdata.pdk.cli;

import picocli.CommandLine;

/**
 * Picocli aims to be the easiest way to create rich command line applications that can run on and off the JVM. Considering picocli? Check what happy users say about picocli.
 * https://picocli.info/
 *
 * @author aplomb
 */
public class TDDMongoDBMain1 {
    //
    public static void main(String... args) {
        args = new String[]{
//                "tdd", "-c", "B:\\code\\tapdata\\idaas-pdk\\tapdata-pdk-cli\\src\\main\\resources\\config\\aerospike.json",
//                "tdd", "-c", "B:\\code\\tapdata\\idaas-pdk\\tapdata-pdk-cli\\src\\main\\resources\\config\\doris.json",
//                "tdd", "-c", "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/tapdata-pdk-cli/src/main/resources/config/doris.json",
                "tdd", "-c", "tapdata-pdk-cli/src/main/resources/config/mongodb.json",
//                "-t", "io.tapdata.pdk.tdd.tests.target.CreateTableTest",
//                "-t", "io.tapdata.pdk.tdd.tests.source.ReadTest",
                "-t", "io.tapdata.pdk.tdd.tests.source.BatchReadTest",
//                "B:\\code\\tapdata\\idaas-pdk\\connectors\\aerospike-connector\\target\\aerospike-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/dist/doris-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/dist/empty-connector-v1.1-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/dist/file-connector-v1.0-SNAPSHOT.jar",

                "connectors/mongodb-connector",
        };

        Main.registerCommands().parseWithHandler(new CommandLine.RunLast(), args);
    }
}
