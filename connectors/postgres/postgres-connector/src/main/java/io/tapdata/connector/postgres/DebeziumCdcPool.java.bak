//package io.tapdata.postgres;
//
//import java.util.concurrent.ConcurrentHashMap;
//
//public class DebeziumCdcPool {
//
//    private static final ConcurrentHashMap<String, DebeziumCdcRunner> runnerMap = new ConcurrentHashMap<>(8);
//
//    public static DebeziumCdcRunner getRunner(String runnerName) {
//        return runnerMap.get(runnerName);
//    }
//
//    public static void runnerStart(String runnerName) {
//        DebeziumCdcRunner runner = getRunner(runnerName);
//        if (runner != null && !runner.isRunning()) {
//            runner.startCdcRunner();
//        }
//    }
//
//    public static void runnerStop(String runnerName) {
//        DebeziumCdcRunner runner = getRunner(runnerName);
//        if (runner != null && runner.isRunning()) {
//            runner.stopCdcRunner();
//        }
//    }
//
//    public static void addRunner(String runnerName, PostgresCdcRunner runner) {
//        runnerMap.put(runnerName, runner);
//    }
//
//    public static void removeRunner(String runnerName) {
//        DebeziumCdcRunner runner = getRunner(runnerName);
//        runner.closeCdcRunner();
//        runnerMap.remove(runnerName);
//    }
//
//    public static void clear() {
//        runnerMap.keySet().forEach(DebeziumCdcPool::removeRunner);
//        runnerMap.clear();
//    }
//}
