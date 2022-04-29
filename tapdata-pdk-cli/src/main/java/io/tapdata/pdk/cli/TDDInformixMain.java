package io.tapdata.pdk.cli;

import picocli.CommandLine;

/**
 * Mysql数据库测试连接器
 *
 * @author Jarad
 * @date 2022/4/18
 */
public class TDDInformixMain {

    public static void main(String... args) {
        args = new String[]{"test", "-c", "tapdata-pdk-cli/src/main/resources/config/gbase8s.json",
//                "-t", "io.tapdata.pdk.tdd.tests.target.DMLTest",
                "dist/informix-connector-v1.0-SNAPSHOT.jar"
        };
        Main.registerCommands().parseWithHandler(new CommandLine.RunLast(), args);
    }

}
