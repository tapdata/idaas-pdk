package io.tapdata.connector.oracle.cdc;

import io.tapdata.common.CdcRunner;
import io.tapdata.common.DataSourcePool;
import io.tapdata.connector.oracle.OracleJdbcContext;
import io.tapdata.connector.oracle.cdc.logminer.RedoLogMiner;
import io.tapdata.connector.oracle.config.OracleConfig;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.util.List;
import java.util.UUID;

public class OracleCdcRunner implements CdcRunner {

    private RedoLogMiner redoLogMiner;
    private final String uuid = UUID.randomUUID().toString();
    private OracleJdbcContext oracleJdbcContext;

    public OracleCdcRunner() {

    }

    public OracleCdcRunner useConfig(OracleConfig oracleConfig) {
        oracleJdbcContext = (OracleJdbcContext) DataSourcePool.getJdbcContext(oracleConfig, OracleJdbcContext.class, uuid);
        switch (oracleConfig.getLogPluginName()) {
            case "logMiner":
                redoLogMiner = new RedoLogMiner(oracleJdbcContext);
                break;
        }
        return this;
    }

    public OracleCdcRunner init(List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        redoLogMiner.init(
                tableList,
                offsetState,
                recordSize,
                consumer
        );
        return this;
    }

    @Override
    public void startCdcRunner() throws Throwable {
        redoLogMiner.startMiner();
    }

    @Override
    public void closeCdcRunner() throws Throwable {
        redoLogMiner.stopMiner();
        oracleJdbcContext.finish(uuid);
    }

    @Override
    public boolean isRunning() throws Throwable {
        return false;
    }

    @Override
    public void run() {
        try {
            startCdcRunner();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
