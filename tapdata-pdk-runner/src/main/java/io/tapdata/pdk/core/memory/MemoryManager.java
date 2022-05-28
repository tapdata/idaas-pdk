package io.tapdata.pdk.core.memory;

import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.concurrent.TimeUnit;

public class MemoryManager {
    private String path;
    private MemoryManager() {
        path = CommonUtils.getProperty("memory_output_dir", "./tap-dumping");
        ExecutorsManager.getInstance().getScheduledExecutorService().schedule(this::scanDir, 10, TimeUnit.SECONDS);
    }

    private void scanDir() {

    }

    public static MemoryManager build() {
        return new MemoryManager();
    }


}
