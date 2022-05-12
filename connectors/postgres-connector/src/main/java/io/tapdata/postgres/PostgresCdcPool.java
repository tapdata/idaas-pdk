package io.tapdata.postgres;

import java.util.concurrent.ConcurrentHashMap;

public class PostgresCdcPool {

    private static final ConcurrentHashMap<String, PostgresCdcRunner> runnerMap = new ConcurrentHashMap<>(8);

    public static PostgresCdcRunner getRunner(String slotName) {
        return runnerMap.get(slotName);
    }

    public static void runnerStart(String slotName) {
        PostgresCdcRunner runner = getRunner(slotName);
        if (runner != null && !runner.isRunning()) {
            runner.startCdcRunner();
        }
    }

    public static void runnerStop(String slotName) {
        PostgresCdcRunner runner = getRunner(slotName);
        if (runner != null && runner.isRunning()) {
            runner.stopCdcRunner();
        }
    }

    public static void addRunner(String slotName, PostgresCdcRunner runner) {
        runnerMap.put(slotName, runner);
    }

    public static void removeRunner(String slotName) {
        PostgresCdcRunner runner = getRunner(slotName);
        runner.closeCdc();
        runnerMap.remove(slotName);
    }

    public static void clear() {
        runnerMap.keySet().forEach(PostgresCdcPool::runnerStop);
        runnerMap.clear();
    }
}
