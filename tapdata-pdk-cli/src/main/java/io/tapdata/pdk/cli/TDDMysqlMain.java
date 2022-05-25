package io.tapdata.pdk.cli;

import picocli.CommandLine;

/**
 * @author samuel
 * @Description
 * @create 2022-04-27 19:37
 **/
public class TDDMysqlMain {
	public static void main(String... args) {
		args = new String[]{
//                "test", "-c", "B:\\code\\tapdata\\idaas-pdk\\tapdata-pdk-cli\\src\\main\\resources\\config\\aerospike.json",
//                "test", "-c", "B:\\code\\tapdata\\idaas-pdk\\tapdata-pdk-cli\\src\\main\\resources\\config\\doris.json",
//                "test", "-c", "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/tapdata-pdk-cli/src/main/resources/config/doris.json",
				"test", "-c", "tapdata-pdk-cli/src/main/resources/config/mysql.json",
//                "-t", "io.tapdata.pdk.tdd.tests.target.CreateTableTest",
//                "-t", "io.tapdata.pdk.tdd.tests.basic.BasicTest",
//                "-t", "io.tapdata.pdk.tdd.tests.target.DMLTest",
//                "-t", "io.tapdata.pdk.tdd.tests.source.BatchReadTest",
                "-t", "io.tapdata.pdk.tdd.tests.source.StreamReadTest",
//                "B:\\code\\tapdata\\idaas-pdk\\connectors\\aerospike-connector\\target\\aerospike-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/dist/doris-connector-v1.0-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/dist/empty-connector-v1.1-SNAPSHOT.jar",
//                "/Users/aplomb/dev/tapdata/GithubProjects/idaas-pdk/dist/file-connector-v1.0-SNAPSHOT.jar",
//				"-i", "tapdata-api",
//                "-i", "tapdata-pdk-api",
//                "-i", "connectors/connector-core",
                "-i", "connectors/mysql/mysql-core",
				"-m", "/Users/samuel/apache-maven-3.6.1",
				"connectors/mysql/mysql-connector",
		};

		Main.registerCommands().parseWithHandler(new CommandLine.RunLast(), args);
	}
}
