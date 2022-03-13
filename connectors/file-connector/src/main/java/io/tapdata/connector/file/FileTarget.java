package io.tapdata.connector.file;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.event.dml.TapDMLEvent;
import io.tapdata.entity.event.dml.TapInsertDMLEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@TapConnectorClass("target.json")
public class FileTarget extends ConnectorBase implements TapConnector {

    @Override
    public void destroy() {

    }


    @Override
    public void discoverSchema(TapConnectionContext databaseContext, Consumer<List<TapTable>> consumer) {
//        TapTableOptions tableOptions1 = new TapTableOptions();
//        TapTable table1 = new TapTable();
//        tableOptions1.setTable(table1);
//        table1.setId("target1.txt");
//        table1.setName("target1.txt");
//        TapTableOptions tableOptions2 = new TapTableOptions();
//        TapTable table2 = new TapTable();
//        tableOptions2.setTable(table2);
//        table2.setId("target2.txt");
//        table2.setName("target2.txt");
//        tapReadOffsetConsumer.accept(Arrays.asList(tableOptions1, tableOptions2), null);

        consumer.accept(list(
                table("target1.txt")
                        .add(field("id", tapString())),
                table("target2.txt")
                        .add(field("id", tapString())),
                table("empty-table2.txt")
                        .add(field("id", tapString()))
        ));
    }

    @Override
    public void connectionTest(TapConnectionContext databaseContext, Consumer<TestItem> consumer) {
        consumer.accept(testItem("Connection", TestResult.Successfully));
        consumer.accept(testItem("Login", TestResult.Successfully));
    }


    private void handleDML(TapConnectorContext connectorContext, List<TapDMLEvent> tapRecordEvents, Consumer<WriteListResult<TapDMLEvent>> consumer) {
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
                    for (TapDMLEvent recordEvent : tapRecordEvents) {
                        if(recordEvent instanceof TapInsertDMLEvent) {
                            TapInsertDMLEvent insertDMLEvent = (TapInsertDMLEvent) recordEvent;
                            Map<String, Object> recordValue = insertDMLEvent.getAfter();
                            fis.write(toJson(recordValue).getBytes(StandardCharsets.UTF_8));
                            fis.write("\r\n".getBytes(StandardCharsets.UTF_8));
                            try {
                                Thread.sleep(1000L);
                            } catch (InterruptedException interruptedException) {
                                interruptedException.printStackTrace();
                            }
                        }
                    }
                    WriteListResult<TapDMLEvent> writeListResult = new WriteListResult<>();
                    writeListResult.setInsertedCount(tapRecordEvents.size());
                    consumer.accept(writeListResult);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecRegistry codecRegistry) {
        connectorFunctions.supportDML(this::handleDML);
    }
}
