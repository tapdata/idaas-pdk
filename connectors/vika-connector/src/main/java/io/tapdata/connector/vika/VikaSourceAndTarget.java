//package io.tapdata.connector.vika;
//
//import io.tapdata.entity.schema.TapTable;
//import io.tapdata.pdk.apis.TapConnector;
//import io.tapdata.pdk.apis.TapSource;
//import io.tapdata.pdk.apis.TapTarget;
//import io.tapdata.pdk.apis.annotations.TapConnectorClass;
//import io.tapdata.base.ConnectorBase;
//import io.tapdata.pdk.apis.common.DefaultMap;
//import io.tapdata.pdk.apis.context.TapConnectorContext;
//import io.tapdata.pdk.apis.context.TapConnectionContext;
//import io.tapdata.pdk.apis.entity.ConnectionTestResult;
//import io.tapdata.pdk.apis.entity.SupportedTapEvents;
//import io.tapdata.pdk.apis.entity.TapEvent;
//import io.tapdata.pdk.apis.entity.WriteListResult;
//import io.tapdata.pdk.apis.entity.dml.TapRecordEvent;
//import io.tapdata.pdk.apis.functions.ConnectorFunctions;
//import io.tapdata.pdk.apis.functions.TargetFunctions;
//import io.tapdata.pdk.apis.functions.consumers.TapListConsumer;
//import io.tapdata.pdk.apis.functions.consumers.TapReadOffsetConsumer;
//import io.tapdata.pdk.apis.functions.consumers.TapWriteConsumer;
//import io.tapdata.pdk.apis.functions.consumers.TapWriteListConsumer;
//import io.tapdata.pdk.apis.spec.TapNodeSpecification;
//import io.tapdata.pdk.apis.typemapping.TapType;
//
//import java.io.IOException;
//import java.util.*;
//import java.util.function.Consumer;
//
//
//@TapConnectorClass("target.json")
//public class VikaSourceAndTarget extends ConnectorBase implements TapConnector {
//    public static final String FIELD_AUTH_TOKEN = "token";
//    public static final String FIELD_SPACE_ID = "spaceId";
//    public static final String FIELD_DATA_SHEET_ID = "dataSheetId";
//
//    public static final String TYPE_SINGLE_TEXT = "SingleText";
//    public static final String TYPE_TEXT = "Text";
//    public static final String TYPE_SINGLE_SELECT = "SingleSelect";
//    public static final String TYPE_MULTI_SELECT = "MultiSelect";
//    public static final String TYPE_NUMBER = "Number";
//    public static final String TYPE_CURRENCY = "Currency";
//    public static final String TYPE_PERCENT = "Percent";
//    public static final String TYPE_DATE_TIME = "DateTime";
//    public static final String TYPE_ATTACHMENT = "Attachment";
//    public static final String TYPE_RATING = "Rating";
//
//    private String token;
//    private String spaceId;
//    private TapNodeSpecification specification;
//    /**
//     *
//     *  @param connectionContext
//     * @param tapReadOffsetConsumer
//     */
//    @Override
//    public void discoverSchema(TapConnectionContext connectionContext, Consumer<List<TapTable>> consumer) {
//        String token = getAuthToken(connectionContext.getConnectionConfig());
//        String spaceId = getSpaceId(connectionContext.getConnectionConfig());
//        DefaultMap result = curl("curl -X GET \"https://api.vika.cn/fusion/v1/spaces/{}/nodes\" -H  \"Authorization: Bearer {}\"", spaceId, token);
//        int code = result.getValue("code", 888);
//        boolean success = result.getValue("success", false);
//        String message = result.getValue("message", "Unknown");
//        if(success) {
//            Map<String, Object> data = (Map<String, Object>) result.get("data");
//            List<Map<String, Object>> nodes = (List<Map<String, Object>>) data.get("nodes");
//            List<TapTableOptions> tableOptions = new ArrayList<>();
//            if(nodes != null) {
//                for(Map<String, Object> node : nodes) {
//                    String name = (String) node.get("name");
//                    String icon = (String) node.get("icon");
//                    String id = (String) node.get("id");
//                    String type = (String) node.get("type"); //Datasheet
//                    Boolean isFav = (Boolean) node.get("isFav");
//                    if(name != null && id != null) {
//                        DefaultMap fieldResult = curl("curl -X GET " +
//                                "\"https://api.vika.cn/fusion/v1/datasheets/{}/fields\" " +
//                                "-H \"Authorization: Bearer {}\"", id, token);
//
//                        TapTableOptions theTableOptions = new TapTableOptions();
//                        theTableOptions.setSyncModes(Arrays.asList(TapTableOptions.SYNC_MODE_INITIAL, TapTableOptions.SYNC_MODE_INCREMENTAL));
//                        TapTable tapTable = new TapTable();
//                        tapTable.setName(name);
//                        tapTable.setId(id);
//                        theTableOptions.setTable(tapTable);
//                        tableOptions.add(theTableOptions);
//
//                        Map<String, TapField> fieldMap = new LinkedHashMap<>();
//                        tapTable.setNameFieldMap(fieldMap);
//                        List<String> primaryKeys = new ArrayList<>();
//                        tapTable.setPrimaryKeys(primaryKeys);
//
//                        code = result.getValue("code", 888);
//                        success = result.getValue("success", false);
//                        message = result.getValue("message", "Unknown");
//
//                        if(success) {
//                            Map<String, Object> dataMap = (Map<String, Object>) fieldResult.get("data");
//                            if(dataMap != null) {
//                                List<Map<String, Object>> dataSheetFields = (List<Map<String, Object>>) dataMap.get("fields");
//                                if(dataSheetFields != null) {
//                                    int index = 0;
//                                    for(Map<String, Object> fieldFormat : dataSheetFields) {
//                                        Boolean fieldEditable = (Boolean) fieldFormat.get("editable");
//                                        if(fieldEditable != null && !fieldEditable)
//                                            continue;
//                                        String fieldType = (String) fieldFormat.get("type");
//                                        String fieldName = (String) fieldFormat.get("name");
//                                        Boolean isPrimary = (Boolean) fieldFormat.get("isPrimary");
//                                        Map<String, Object> property = (Map<String, Object>) fieldFormat.get("property");
//                                        TapField tapField = new TapField();
//                                        tapField.setColumnPosition(index++);
//                                        tapField.setName(fieldName);
//                                        tapField.setProperties(property);
//                                        tapField.setOriginalType(fieldType);
//                                        if(isPrimary != null && isPrimary) {
//                                            primaryKeys.add(fieldName);
//                                        }
//                                        if(fieldType != null && fieldName != null) {
//                                            switch (fieldType) {
//                                                case TYPE_ATTACHMENT:
//                                                    tapField.setTapType(TapType.Array.name());
//                                                    tapField.setOriginConvertor(value -> {
//                                                        List<String> array = new ArrayList<>();
//                                                        List<Map<String, Object>> attachments = (List<Map<String, Object>>) value;
//                                                        if(attachments != null) {
//                                                            for(Map<String, Object> attachment : attachments) {
//                                                                String attachmentName = (String) attachment.get("name");
//                                                                String url = (String) attachment.get("url");
//                                                                if(attachmentName != null && url != null)
//                                                                    array.add(attachmentName + ":" + url);
//                                                            }
//                                                        }
//                                                        return array;
//                                                    });
//                                                    break;
//                                                case TYPE_DATE_TIME:
//                                                    tapField.setTapType(TapType.Time.name());
//                                                    break;
//                                                case TYPE_MULTI_SELECT:
//                                                    tapField.setTapType(TapType.Array.name());
//                                                    break;
//                                                case TYPE_CURRENCY:
//                                                case TYPE_NUMBER:
//                                                case TYPE_RATING:
//                                                case TYPE_PERCENT:
//                                                    tapField.setTapType(TapType.Number.name());
//                                                    break;
//                                                case TYPE_SINGLE_SELECT:
//                                                case TYPE_SINGLE_TEXT:
//                                                case TYPE_TEXT:
//                                                    tapField.setTapType(TapType.String.name());
//                                                    break;
//                                                default:
//                                                    //Ignore
//                                                    continue;
//                                            }
//                                        }
//                                        fieldMap.put(tapField.getName(), tapField);
//                                    }
//                                }
//                            }
//                        } else {
//                            throw new RuntimeException("Alltables failed, code " + code + " message " + message);
//                        }
//                    }
//                }
//            }
//            tapReadOffsetConsumer.accept(tableOptions, null);
//        } else {
//            tapReadOffsetConsumer.accept(null, new IOException("Alltables failed, code " + code + " message " + message));
//        }
//    }
//
//    private String getAuthToken(DefaultMap connectionConfig) {
//        if(token != null)
//            return token;
//        token = (String) connectionConfig.get(FIELD_AUTH_TOKEN);
//        if(token == null)
//            throw new IllegalArgumentException("Auth token is missing");
//        return token;
//    }
//
//    private String getSpaceId(DefaultMap connectionConfig) {
//        if(spaceId != null)
//            return spaceId;
//        spaceId = (String) connectionConfig.get(FIELD_SPACE_ID);
//        if(spaceId == null)
//            throw new IllegalArgumentException("SpaceId is missing");
//        return spaceId;
//    }
//
//    @Override
//    public ConnectionTestResult connectionTest(TapConnectionContext databaseContext) {
//        boolean success = false;
//        String message = null;
//        int code = 0;
//        try {
//            String token = getAuthToken(databaseContext.getConnectionConfig());
//            String spaceId = getSpaceId(databaseContext.getConnectionConfig());
//            DefaultMap result = curl("curl -X GET \"https://api.vika.cn/fusion/v1/spaces/{}/nodes\" -H  \"Authorization: Bearer {}\"", spaceId, token);
//            code = result.getValue("code", 888);
//            success = result.getValue("success", false);
//            message = result.getValue("message", "Unknown");
//        } catch (Throwable throwable) {
//            message = throwable.getMessage();
//        }
//
//        ConnectionTestResult connectionTestResult = new ConnectionTestResult();
//        List<ConnectionTestResult.TestItem> testItems = new ArrayList<>();
//        connectionTestResult.setTestPoints(testItems);
//        ConnectionTestResult.TestItem item = new ConnectionTestResult.TestItem();
//        testItems.add(item);
//        if(success) {
//            connectionTestResult.setResult(ConnectionTestResult.RESULT_SUCCESSFULLY);
//            item.setResult(ConnectionTestResult.TestItem.RESULT_PASS);
//            item.setItem("Connection");
//            item.setInformation("OK");
//        } else {
//            connectionTestResult.setResult(ConnectionTestResult.RESULT_FAILED);
//            item.setResult(ConnectionTestResult.TestItem.RESULT_FAILED);
//            item.setItem("Connection");
//            item.setInformation("Code " + code + " message " + message);
//        }
//        return connectionTestResult;
//    }
//
//    @Override
//    public void destroy() {
//
//    }
//
//    @Override
//    public void init(TapConnectorContext connectorContext, TapNodeSpecification specification) {
//        this.specification = specification;
//    }
//
//    @Override
//    public void sourceFunctions(ConnectorFunctions connectorFunctions) {
//        connectorFunctions.withBatchCountFunction(this::batchCount);
//        connectorFunctions.withBatchReadFunction(this::batchRead);
////        sourceFunctions.withStreamReadFunction(this::streamRead);
//    }
//
//    private void batchRead(TapConnectorContext connectorContext, Object offset, TapReadOffsetConsumer<TapEvent> tapEventTapReadOffsetConsumer) {
//        String token = getAuthToken(connectorContext.getConnectionConfig());
//        String spaceId = getSpaceId(connectorContext.getConnectionConfig());
//        String dataSheetId = getDataSheetId(connectorContext.getTable(), token, spaceId);
//        int pageSize = 1000;
//        int pageNum = 1;
//        if(offset != null)
//            pageNum = (int) offset;
//        while(true) {
//            DefaultMap result = curl("curl \"https://api.vika.cn/fusion/v1/datasheets/{}/records?viewId=viwKjSB371XWa&fieldKey=name&pageNum={}&pageSize={}\" \\\n" +
//                    "  -H \"Authorization: Bearer {}\"\n", dataSheetId, Integer.toString(pageNum), Integer.toString(pageSize), token);
//            int code = result.getValue("code", 888);
//            boolean success = result.getValue("success", false);
//            String message = result.getValue("message", "Unknown");
//            if(success) {
//                Map<String, Object> data = (Map<String, Object>) result.get("data");
//                if(data != null) {
//                    List<Map<String, Object>> records = (List<Map<String, Object>>) data.get("records");
//                    if(records != null) {
//                        List<TapEvent> recordEventList = new ArrayList<>();
//                        for(Map<String, Object> record : records) {
//                            Map<String, Object> fields = (Map<String, Object>) record.get("fields");
//                            if(fields != null) {
//                                TapRecordEvent recordEvent = TapRecordEvent.create(TapRecordEvent.TYPE_INSERT, connectorContext);
//                                recordEvent.setAfter(recordEvent.formatValue(fields, connectorContext.getTable().getNameFieldMap()));
//                                recordEvent.setReferenceTime(recordEvent.getTime());
//                                recordEventList.add(recordEvent);
//                            }
//                        }
//                        if(recordEventList.size() < pageSize) {
//                            tapEventTapReadOffsetConsumer.accept(recordEventList, pageNum, null, true);
//                            break;
//                        } else {
//                            tapEventTapReadOffsetConsumer.accept(recordEventList, pageNum, null, false);
//                        }
//                    }
//                }
//            } else {
//                throw new RuntimeException("BatchRead failed, code " + code + " message " + message);
//            }
//        }
//    }
//
//    private long batchCount(TapConnectorContext connectorContext, Object offset) {
//        String token = getAuthToken(connectorContext.getConnectionConfig());
//        String spaceId = getSpaceId(connectorContext.getConnectionConfig());
//        String dataSheetId = getDataSheetId(connectorContext.getTable(), token, spaceId);
//        DefaultMap result = curl("curl \"https://api.vika.cn/fusion/v1/datasheets/{}/records?viewId=viwKjSB371XWa&fieldKey=name\" \\\n" +
//                "  -H \"Authorization: Bearer {}\"\n", dataSheetId, token);
//        int code = result.getValue("code", 888);
//        boolean success = result.getValue("success", false);
//        String message = result.getValue("message", "Unknown");
//        if(success) {
//            Map<String, Object> data = (Map<String, Object>) result.get("data");
//            if(data != null) {
//                Number total = (Number) data.get("total");
//                if(total != null)
//                    return total.longValue();
//            }
//        } else {
//            throw new RuntimeException("BatchCount failed, code " + code + " message " + message);
//        }
//        return 0;
//    }
//
//    @Override
//    public void targetFunctions(TargetFunctions targetFunctions, SupportedTapEvents supportedTapEvents) {
//        supportedTapEvents
//                .supportDMLTypes(Arrays.asList(TapRecordEvent.TYPE_INSERT))
//                .notSupportSchemaTypes()
//                .supportTableTypes(Arrays.asList(TapTableEvent.TYPE_CLEAR_TABLE));
//        targetFunctions
//                .withDMLFunction(this::handleDML)
//                .withDDLFunction(this::handleDDL)
//        ;
//    }
//
//    private void handleDDL(TapConnectorContext connectorContext, TapDDLEvent tapDDLEvent, TapWriteConsumer<TapDDLEvent> tapDDLEventTapWriteConsumer) {
//        if(tapDDLEvent instanceof TapTableEvent) {
//            TapTableEvent tableEvent = (TapTableEvent) tapDDLEvent;
//            switch (tableEvent.getType()) {
//                case TapTableEvent.TYPE_CLEAR_TABLE:
//                    String token = getAuthToken(connectorContext.getConnectionConfig());
//                    String spaceId = getSpaceId(connectorContext.getConnectionConfig());
//                    String dataSheetId = getDataSheetId(connectorContext.getTable(), token, spaceId);
//
//                    DefaultMap result = curl("curl \"https://api.vika.cn/fusion/v1/datasheets/{}/records?viewId=viwKjSB371XWa&fieldKey=name&pageNum={}&pageSize={}\" \\\n" +
//                            "  -H \"Authorization: Bearer {}\"\n", dataSheetId, 1, 10, token);
//                    int code = result.getValue("code", 888);
//                    boolean success = result.getValue("success", false);
//                    String message = result.getValue("message", "Unknown");
//                    if(!success) {
//                        throw new RuntimeException("Query records for deletion failed, " + message + " code " + code);
//                    }
//                    Map<String, Object> data = (Map<String, Object>) result.get("data");
//                    if (data != null) {
//                        List<Map<String, Object>> records = (List<Map<String, Object>>) data.get("records");
//                        if (records != null) {
//                            List<String> deleteIds = new ArrayList<>();
//                            for (Map<String, Object> record : records) {
//                                String recordId = (String) record.get("recordId");
////                                    Long createTime = (Long) record.get("createdAt");
////                                    Long updateTime = (Long) record.get("updatedAt");
//                                if (recordId != null && !deleteIds.contains(recordId)) {
//                                    deleteIds.add(recordId);
//                                }
//                            }
//
//                            StringBuilder builder = new StringBuilder();
//                            for(int i = 0; i < deleteIds.size(); i++) {
//                                String id = deleteIds.get(i);
//                                builder.append("recordIds=").append(id);
//                                if(i < deleteIds.size() - 1)
//                                    builder.append("&");
//                            }
//
//                            result = curl("curl -X DELETE \"https://api.vika.cn/fusion/v1/datasheets/{}/records?{}\" \n" +
//                                    "  -H \"Authorization: Bearer {}\"\n", dataSheetId, builder.toString(), token);
//                            code = result.getValue("code", 888);
//                            success = result.getValue("success", false);
//                            message = result.getValue("message", "Unknown");
//                            if(!success) {
//                                throw new RuntimeException("Delete records " + Arrays.toString(deleteIds.toArray()) + " failed, " + message + " code " + code);
//                            }
//                        }
//                    }
//                    break;
//            }
//        }
//    }
//
//    private void handleDML(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapWriteListConsumer<TapRecordEvent> consumer) {
//        String token = getAuthToken(connectorContext.getConnectionConfig());
//        String spaceId = getSpaceId(connectorContext.getConnectionConfig());
//        String dataSheetId = getDataSheetId(connectorContext.getTable(), token, spaceId);
//
//        if(tapRecordEvents != null) {
//            List<Map<String, Object>> values = new ArrayList<>();
//            for(TapRecordEvent recordEvent : tapRecordEvents) {
//                switch (recordEvent.getType()) {
//                    case TapRecordEvent.TYPE_INSERT:
//                        values.add(convertValue(recordEvent, connectorContext.getTable().getNameFieldMap()));
//                        break;
//                }
//                //创建记录接口：一次最多创建 10 行记录。
//                if(values.size() == 10) {
//                    insert(token, dataSheetId, values, consumer);
//                    values.clear();
//                }
//            }
//            if(!values.isEmpty()) {
//                insert(token, dataSheetId, values, consumer);
//            }
//        }
//    }
//
//    private void insert(String token, String dataSheetId, List<Map<String, Object>> values, TapWriteListConsumer<TapRecordEvent> consumer) {
//        DefaultMap result = curl("curl -X POST \"https://api.vika.cn/fusion/v1/datasheets/{}/records?viewId=viwKjSB371XWa&fieldKey=name\"  \\\n" +
//                "  -H \"Authorization: Bearer {}\" \\\n" +
//                "  -H \"Content-Type: application/json\" \\\n" +
//                "  --data '{\n" +
//                "  \"records\": {},\n" +
//                "  \"fieldKey\": \"name\"\n" +
//                "}'", dataSheetId, token, toJson(values));
//        int code = result.getValue("code", 888);
//        boolean success = result.getValue("success", false);
//        String message = result.getValue("message", "Unknown");
//        if(success) {
//            WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>();
//            writeListResult.setInsertedCount(values.size());
//            consumer.accept(writeListResult, null);
//        }
//        //Sleep to slow down
//        //同一个用户对同一张表的 API 请求频率上限为 5 次/秒。
//        try {
//            Thread.sleep(500L);
//        } catch (InterruptedException interruptedException) {
//            interruptedException.printStackTrace();
//        }
//        if(!success) {
//            throw new RuntimeException("Handle dml failed, code " + code + " message " + message);
//        }
//    }
//
//    private Map<String, Object> convertValue(TapRecordEvent recordEvent, Map<String, TapField> fieldMap) {
//        Map<String, Object> recordValue = recordEvent.getAfter();
//        if(recordValue == null) {
//            return null;
//        }
//        Map<String, Object> newValue = new LinkedHashMap<>();
//        Set<String> keys = fieldMap.keySet();
//        for(String key : keys) {
//            Object fieldValue = recordValue.get(key);
//            TapField field = fieldMap.get(key);
//            if(field != null && fieldValue != null) {
//                switch (field.getOriginalType()) {
//                    case TYPE_ATTACHMENT:
//                        //Ignore
//                        if(specification.getId().equals(recordEvent.getPdkId()) && specification.getGroup().equals(recordEvent.getPdkGroup())) {
//                            Map<String, Object> originMap = recordEvent.getOriginFieldMap();
//                            if(originMap != null) {
////                                newValue.put(key, originMap.get(key));
//                            }
//                        }
//                        break;
//                    case TYPE_DATE_TIME:
//                    case TYPE_RATING:
//                        newValue.put(key, toLong(fieldValue));
//                        break;
//                    case TYPE_MULTI_SELECT:
//                        newValue.put(key, toStringArray(fieldValue));
//                        break;
//                    case TYPE_CURRENCY:
//                    case TYPE_NUMBER:
//                    case TYPE_PERCENT:
//                        newValue.put(key, toDouble(fieldValue));
//                        break;
//                    case TYPE_SINGLE_SELECT:
//                    case TYPE_SINGLE_TEXT:
//                    case TYPE_TEXT:
//                        newValue.put(key, toString(fieldValue));
//                        break;
//                    default:
//                        //Ignore
//                        continue;
//                }
//            }
//        }
//        Map<String, Object> theValue = new HashMap<>();
//        theValue.put("fields", newValue);
//        return theValue;
//    }
//
//    private String getDataSheetId(TapTable table, String token, String spaceId) {
//        String dataSheetId = table.getId();
//        if(token == null || spaceId == null || dataSheetId == null) {
//            throw new IllegalArgumentException(VikaSourceAndTarget.class.getSimpleName() + ": handleDML failed, illegal parameters, token " + token + " spaceId " + spaceId + " dataSheetId " + dataSheetId);
//        }
//        return dataSheetId;
//    }
//}
