package io.tapdata.pdk.cli;

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
//                "register", "-a", "3324cfdf-7d3e-4792-bd32-571638d4562f", "-t", "http://192.168.1.132:31787",
//                "register", "-a", "3324cfdf-7d3e-4792-bd32-571638d4562f", "-t", "http://localhost:3000",

//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/dist/empty-connector-v1.1-SNAPSHOT.jar",
//                "D:/workspace/idaas-pdk/dist/mysql-connector-v1.0-SNAPSHOT.jar",
                "D:/workspace/idaas-pdk/dist/postgres-connector-v1.0-SNAPSHOT.jar",
//                "D:/workspace/idaas-pdk/dist/mongodb-connector-v1.0-SNAPSHOT.jar",
        };

        Main.registerCommands().parseWithHandler(new CommandLine.RunLast(), args);
    }
}
