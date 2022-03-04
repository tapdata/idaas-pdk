package io.tapdata.connector.file;

import io.tapdata.base.ConnectorBase;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionTestResult;
import io.tapdata.pdk.apis.entity.SupportedTapEvents;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.entity.ddl.TapTable;
import io.tapdata.pdk.apis.entity.dml.TapRecordEvent;
import io.tapdata.pdk.apis.functions.consumers.TapListConsumer;
import io.tapdata.pdk.apis.functions.consumers.TapWriteListConsumer;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.apis.TapTarget;
import io.tapdata.pdk.apis.annotations.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.TargetFunctions;
import io.tapdata.pdk.apis.entity.ddl.TapTableOptions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@TapConnector("target.json")
public class FileTarget extends ConnectorBase implements TapTarget {

    @Override
    public void destroy() {

    }

    @Override
    public void init(TapConnectorContext connectorContext, TapNodeSpecification specification) {

    }

    @Override
    public void discoverSchema(TapConnectionContext databaseContext, TapListConsumer<TapTableOptions> tapReadOffsetConsumer) {
        TapTableOptions tableOptions1 = new TapTableOptions();
        TapTable table1 = new TapTable();
        tableOptions1.setTable(table1);
        table1.setId("target1.txt");
        table1.setName("target1.txt");
        TapTableOptions tableOptions2 = new TapTableOptions();
        TapTable table2 = new TapTable();
        tableOptions2.setTable(table2);
        table2.setId("target2.txt");
        table2.setName("target2.txt");
        tapReadOffsetConsumer.accept(Arrays.asList(tableOptions1, tableOptions2), null);
    }

    @Override
    public ConnectionTestResult connectionTest(TapConnectionContext databaseContext) {
        return null;
    }

    @Override
    public void targetFunctions(TargetFunctions targetFunctions, SupportedTapEvents supportedTapEvents) {
        targetFunctions.withDMLFunction(this::handleDML);
    }

    private void handleDML(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapWriteListConsumer<TapRecordEvent> consumer) {
        TapTable table = connectorContext.getTable();
        String folderPath = (String) connectorContext.getConnectionConfig().get("folderPath");
        if(table == null || table.getName() == null)
            throw new IllegalArgumentException("Table is null or name is null. ");
        if(folderPath == null)
            throw new IllegalArgumentException("Folder path is null");

        String path = FilenameUtils.concat(folderPath, table.getName());
        File file = new File(path);
        if(file.isDirectory())
            throw new IllegalArgumentException("Table file " + path + " is directory, need to be a file. ");
        try {
            try (FileOutputStream fis = FileUtils.openOutputStream(file, true)) {
                if (tapRecordEvents != null) {
                    for (TapRecordEvent recordEvent : tapRecordEvents) {
                        switch (recordEvent.getType()) {
                            case TapRecordEvent.TYPE_INSERT:
                                Map<String, Object> recordValue = recordEvent.getAfter();
                                fis.write(toJson(recordValue).getBytes(StandardCharsets.UTF_8));
                                fis.write("\r\n".getBytes(StandardCharsets.UTF_8));
                                try {
                                    Thread.sleep(1000L);
                                } catch (InterruptedException interruptedException) {
                                    interruptedException.printStackTrace();
                                }
                                break;
                        }
                    }
                    WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>();
                    writeListResult.setInsertedCount(tapRecordEvents.size());
                    consumer.accept(writeListResult, null);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
