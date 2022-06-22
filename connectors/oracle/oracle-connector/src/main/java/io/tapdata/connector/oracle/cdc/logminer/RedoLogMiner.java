package io.tapdata.connector.oracle.cdc.logminer;

import io.tapdata.connector.oracle.OracleJdbcContext;
import io.tapdata.connector.oracle.cdc.logminer.bean.OracleTransaction;
import io.tapdata.connector.oracle.cdc.logminer.bean.RedoLog;
import io.tapdata.connector.oracle.cdc.logminer.bean.RedoLogContent;
import io.tapdata.connector.oracle.cdc.logminer.parser.ParseSQLRedoLogParser;
import io.tapdata.connector.oracle.cdc.offset.OracleOffset;
import io.tapdata.connector.oracle.config.OracleConfig;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import oracle.jdbc.OracleResultSet;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.tapdata.connector.oracle.cdc.logminer.OracleSqlConstant.*;

public class RedoLogMiner {

    private final static String TAG = RedoLogMiner.class.getSimpleName();
    private final DateTimeColumnHandler dateTimeColumnHandler;
    private static final BeanUtils beanUtils = InstanceFactory.instance(BeanUtils.class); //bean util
    private final OracleJdbcContext oracleJdbcContext;
    private Statement statement;
    private final OracleConfig oracleConfig;
    private final String version;
    private Object offsetState;
    private List<String> tableList;
    private int recordSize;
    private StreamReadConsumer consumer;
    private LinkedBlockingQueue<RedoLogContent> logQueue;
    private final LinkedHashMap<String, OracleTransaction> transactionBucket = new LinkedHashMap<>();
    private final Map<Integer, Long> instancesMindedScnMap = new HashMap<>();
    private boolean hasRollbackTemp;
    protected RedoLogContent csfLogContent = null;
    private final Map<Long, String> tableObjectId = new HashMap<>();
    private ResultSet resultSet = null;
    private boolean isRunning = false;
    private Thread analyzeThread = null;
    private Map<String, Integer> columnTypeMap = new HashMap<>();
    private Map<String, String> dateTimeTypeMap = new HashMap<>();
    // https://docs.oracle.com/cd/E16338_01/appdev.112/e13995/constant-values.html#oracle_jdbc_OracleTypes_TIMESTAMPTZ
    private static final int TIMESTAMP_TZ_TYPE = -101;
    // https://docs.oracle.com/cd/E16338_01/appdev.112/e13995/constant-values.html#oracle_jdbc_OracleTypes_TIMESTAMPLTZ
    private static final int TIMESTAMP_LTZ_TYPE = -102;

    private final static int ROLLBACK_TEMP_LIMIT = 50;
    private final static int LOG_QUEUE_SIZE = 1000;

    public RedoLogMiner(OracleJdbcContext oracleJdbcContext) {
        this.oracleJdbcContext = oracleJdbcContext;
        oracleConfig = (OracleConfig) oracleJdbcContext.getConfig();
        dateTimeColumnHandler = new DateTimeColumnHandler(oracleConfig.getSysZoneId());
        version = oracleJdbcContext.queryVersion();
    }

    public void init(List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        this.offsetState = offsetState;
        this.tableList = tableList;
        this.recordSize = recordSize;
        this.consumer = consumer;
        getColumnType();
    }

    public void getColumnType() {
        tableList.forEach(table -> {
            try {
                oracleJdbcContext.query("SELECT * FROM \"" + oracleConfig.getSchema() + "\".\"" + table + "\"", resultSet -> {
                    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                        int colType = resultSetMetaData.getColumnType(i);
                        columnTypeMap.put(table + "." + resultSetMetaData.getColumnName(i), colType);
                        if (colType == Types.DATE || colType == Types.TIME || colType == Types.TIMESTAMP) {
                            dateTimeTypeMap.put(table + "." + resultSetMetaData.getColumnName(i), resultSetMetaData.getColumnTypeName(i));
                        }
                    }
                });
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void startMiner() throws Throwable {
        //1、check whether archived log exists
        AtomicBoolean notExistsArchivedLog = new AtomicBoolean(false);
        TapLogger.info(TAG, "Checking whether archived log exists...");
        oracleJdbcContext.query(CHECK_ARCHIVED_LOG_EXISTS, resultSet -> notExistsArchivedLog.set(resultSet.getInt(1) <= 0));
        //2、find lastScn from offset
        Long lastScn;
        boolean isMax;
        if (EmptyKit.isNull(offsetState) || EmptyKit.isNull(((OracleOffset) offsetState).getLastScn())) {
            lastScn = findCurrentScn();
            isMax = false;
        } else {
            OracleOffset offset = (OracleOffset) offsetState;
            isMax = offset.getLastScn() == Long.MAX_VALUE;
            lastScn = offset.getPendingScn() > 0 && offset.getPendingScn() < offset.getLastScn() ? offset.getPendingScn() : offset.getLastScn();
        }
        //3、if there is no archived log, get first online log
        if (notExistsArchivedLog.get()) {
            RedoLog redoLog = firstOnlineRedoLog(lastScn);
            if (EmptyKit.isNull(redoLog)) {
                throw new Exception(String.format("SCN %s is not included in the online log", lastScn));
            }
            //4、add log file for logMiner
            oracleJdbcContext.execute(String.format(ADD_REDO_LOG_FILE_FOR_LOGMINER, redoLog.getName()));
            lastScn = lastScn < redoLog.getFirstChangeScn() ? redoLog.getFirstChangeScn() : lastScn;
        }
        //5、if necessary store in dictionary
        oracleJdbcContext.query(String.format(LAST_DICT_ARCHIVE_LOG_BY_SCN, lastScn), resultSet -> {
            if (resultSet.getRow() <= 0) {
                TapLogger.info(TAG, "building new log file...");
                oracleJdbcContext.execute(STORE_DICT_IN_REDO_SQL);
            }
        });
        if (EmptyKit.isNotBlank(oracleConfig.getPdb())) {
            TapLogger.info(TAG, "database is containerised, switching...");
            oracleJdbcContext.execute(SWITCH_TO_CDB_ROOT);
        }
        //7、start logMiner
        statement = oracleJdbcContext.getConnection().createStatement();
        TapLogger.info(TAG, "Redo Log Miner is starting...");
        statement.execute(String.format(START_LOG_MINOR_CONTINUOUS_MINER_SQL, lastScn));
        logQueue = new LinkedBlockingQueue<>(LOG_QUEUE_SIZE);
        isRunning = true;
        TapLogger.info(TAG, "Redo Log Miner has been started...");
        analyzeThread = new Thread(() -> {
            RedoLogContent redoLogContent;
            while (isRunning) {
                try {
                    redoLogContent = logQueue.poll(1, TimeUnit.SECONDS);
                    if (redoLogContent == null) {
                        continue;
                    }
                } catch (Exception e) {
                    break;
                }
                try {

                    // parse sql
                    if (canParse(redoLogContent)) {
                        RedoLogContent.OperationEnum operationEnum = RedoLogContent.OperationEnum.fromOperationCode(redoLogContent.getOperationCode());
                        String sqlRedo = null;
                        // oracle的bug，删除事件的redo会出现 delete from xxx where and a=1的语法错误问题，因此删除事件通过解析undo的方式实现
                        if (operationEnum == RedoLogContent.OperationEnum.DELETE) {
                            sqlRedo = redoLogContent.getSqlUndo();
                            operationEnum = RedoLogContent.OperationEnum.INSERT;
                        } else {
                            sqlRedo = redoLogContent.getSqlRedo();
                        }
                        redoLogContent.setRedoRecord(new ParseSQLRedoLogParser().parseSQL(sqlRedo, operationEnum));
                        convertStringToObject(redoLogContent);

//                        String mongodbBefore = context.getSettingService().getString("mongodb.before");

//                        if (
//                                redoLogParser.needParseUndo(redoLogContent.getOperation(),
//                                        redoLogContent.getSqlUndo(),
//                                        mongodbBefore,
//                                        context.getJobTargetConn() != null && context.getJobTargetConn().isSupportUpdatePk(),
//                                        context.getJob().getNoPrimaryKey()
//                                )
//                        ) {
//                            redoLogContent.setUndoRecord(redoLogParser.parseSQL(redoLogContent.getSqlUndo(), RedoLogContent.OperationEnum.UPDATE));
//                        }
                    }

                    // process and callback
                    if (!"COMMIT".equals(redoLogContent.getOperation()) && !"ROLLBACK".equals(redoLogContent.getOperation())) {
                        System.out.println(redoLogContent);
                    }
                    processOrBuffRedoLogContent(redoLogContent, this::sendTransaction);

                } catch (Exception e) {
                    e.printStackTrace();
                    consumer.streamReadEnded();
                }
            }
        });
        consumer.streamReadStarted();
        analyzeThread.start();
        //8、begin to analyze logs
        analyzeResultSet(lastScn, isMax);
    }

    public void stopMiner() throws Throwable {
        TapLogger.info(TAG, "Log Miner is shutting down...");
        isRunning = false;
        Thread.sleep(500);
        resultSet.close();
        resultSet = null;
        statement.execute(END_LOG_MINOR_SQL);
        analyzeThread.interrupt();
    }

    private long findCurrentScn() throws Throwable {
        AtomicLong currentScn = new AtomicLong();
        String sql = version.equals("9i") ? CHECK_CURRENT_SCN_9I : CHECK_CURRENT_SCN;
        oracleJdbcContext.query(sql, resultSet -> currentScn.set(resultSet.getLong(1)));
        return currentScn.get();
    }

    private RedoLog firstOnlineRedoLog(long scn) throws Throwable {
        AtomicReference<RedoLog> redoLog = new AtomicReference<>();
        boolean useOldVersionSql = StringUtils.equalsAnyIgnoreCase(version, "9i", "10g");
        String firstOnlineSQL = useOldVersionSql ? GET_FIRST_ONLINE_REDO_LOG_FILE_FOR_10G_AND_9I : GET_FIRST_ONLINE_REDO_LOG_FILE;
        if (scn > 0) {
            firstOnlineSQL = useOldVersionSql ? String.format(GET_FIRST_ONLINE_REDO_LOG_FILE_BY_SCN_FOR_10G_AND_9I, scn) : String.format(GET_FIRST_ONLINE_REDO_LOG_FILE_BY_SCN, scn);
        }
        oracleJdbcContext.query(firstOnlineSQL, resultSet -> redoLog.set(RedoLog.onlineLog(resultSet, version)));

        return redoLog.get();
    }

    private Set<String> getTableObjectIds() throws Throwable {
        String sql = String.format(GET_TABLE_OBJECT_ID_WITH_CLAUSE, "'" + oracleConfig.getSchema() + "'", StringKit.joinString(tableList, "'", ", "));
        Set<String> tableObjectIds = new HashSet<>();
        oracleJdbcContext.query(sql, resultSet -> {
            while (!resultSet.isAfterLast() && resultSet.getRow() > 0) {
                tableObjectIds.add(resultSet.getString("OBJECT_ID"));
                tableObjectId.put(resultSet.getLong(2), resultSet.getString(1));
            }
        });
        return tableObjectIds;
    }

    private String analyzeLogSql(Long scn, boolean isMax) {
        String sql;
        if (version.equals("9i")) {
            sql = GET_REDO_LOG_RESULT_ORACLE_LOG_COLLECT_SQL_9i;
        } else {
            sql = GET_REDO_LOG_RESULT_ORACLE_LOG_COLLECT_SQL;
        }
        if (version.equals("9i")) {
            Set<String> tableObjectIds;
            try {
                tableObjectIds = getTableObjectIds();
            } catch (Throwable throwable) {
                throw new RuntimeException(String.format("Get table object id failed, message: %s", throwable.getMessage()));
            }
            String tableObjectIdsInClause = " AND OBJECT_ID IN (" + StringKit.joinString(tableObjectIds, "'", ", ") + ")";
            sql = String.format(sql, scn, " AND SEG_OWNER='" + oracleConfig.getSchema() + "'", tableObjectIdsInClause);
        } else {
            sql = String.format(
                    sql,
                    "19c".equals(version) && StringUtils.isNotBlank(oracleConfig.getPdb()) ? " SRC_CON_NAME = UPPER('" + oracleConfig.getPdb() + "') AND " : "",
                    scn,
                    " AND SEG_OWNER='" + oracleConfig.getSchema() + "'",
                    " AND TABLE_NAME IN (" + StringKit.joinString(tableList, "'", ", ") + ")"
            );
        }
        return sql;
    }

    private void analyzeResultSet(Long scn, boolean isMax) {
        try {
            statement.setFetchSize(oracleConfig.getFetchSize());
            resultSet = statement.executeQuery(analyzeLogSql(scn, isMax));
            while (isRunning && EmptyKit.isNotNull(resultSet) && resultSet.next()) {
                RedoLogContent redoLogContent = wrapRedoLogContent(resultSet);
                if (!validateRedoLogContent(redoLogContent)) {
                    continue;
                }
                if (csfRedoLogProcess(resultSet, redoLogContent)) {
                    continue;
                }
                String operation = redoLogContent.getOperation();
                if (OracleConstant.REDO_LOG_OPERATION_LOB_TRIM.equals(operation)
                        || OracleConstant.REDO_LOG_OPERATION_LOB_WRITE.equals(operation)) {
                    continue;
                }
                if (OracleConstant.REDO_LOG_OPERATION_UNSUPPORTED.equals(operation)) {
                    continue;
                }
                enqueueRedoLogContent(redoLogContent);
            }
        } catch (SQLException e) {
            TapLogger.warn(TAG, "Log Miner has been stopped by closing resultSet!");
        }
    }

    private RedoLogContent wrapRedoLogContent(ResultSet resultSet) throws SQLException {
        if (csfLogContent == null) {
            return buildRedoLogContent(resultSet);
        } else {
            return appendRedoAndUndoSql(resultSet);
        }
    }

    private RedoLogContent buildRedoLogContent(ResultSet resultSet) throws SQLException {
        RedoLogContent redoLogContent;
        if (version.equals("9i")) {
            redoLogContent = new RedoLogContent(resultSet, tableObjectId, oracleConfig.getSysZoneId());
        } else {
            redoLogContent = new RedoLogContent(resultSet, oracleConfig.getSysZoneId());
        }
        return redoLogContent;
    }

    private RedoLogContent appendRedoAndUndoSql(ResultSet resultSet) throws SQLException {
        if (resultSet == null) {
            return null;
        }
        String redoSql = resultSet.getString("SQL_REDO");
        String undoSql = resultSet.getString("SQL_UNDO");
        if (StringUtils.isNotBlank(redoSql)) {
            csfLogContent.setSqlRedo(csfLogContent.getSqlRedo() + redoSql);
        }
        if (StringUtils.isNotBlank(undoSql)) {
            csfLogContent.setSqlUndo(csfLogContent.getSqlUndo() + undoSql);
        }
        RedoLogContent redoLogContent = new RedoLogContent();
        beanUtils.copyProperties(csfLogContent, redoLogContent);
        return redoLogContent;
    }

    public boolean validateRedoLogContent(RedoLogContent redoLogContent) {
        if (redoLogContent == null) {
            return false;
        }

        if (!StringUtils.equalsAnyIgnoreCase(redoLogContent.getOperation(),
                OracleConstant.REDO_LOG_OPERATION_COMMIT, OracleConstant.REDO_LOG_OPERATION_ROLLBACK)) {
            // check owner
            if (StringUtils.isNotBlank(redoLogContent.getSegOwner())
                    && !oracleConfig.getSchema().equals(redoLogContent.getSegOwner())) {
                return false;
            }
            // check table name
            return !EmptyKit.isNotBlank(redoLogContent.getTableName()) || tableList.contains(redoLogContent.getTableName());
        }

        return true;
    }

    private boolean csfRedoLogProcess(ResultSet resultSet, RedoLogContent redoLogContent) {
        // handle continuation redo/undo sql
        if (isCsf(resultSet)) {
            if (csfLogContent == null) {
                csfLogContent = new RedoLogContent();
                beanUtils.copyProperties(redoLogContent, csfLogContent);
            }
            return true;
        } else {
            csfLogContent = null;
        }
        return false;
    }

    public boolean isCsf(Object logData) {
        if (logData != null) {
            try {
                Integer csf = null;

                if (logData instanceof OracleResultSet) {
                    csf = ((ResultSet) logData).getInt("CSF");
                } else if (logData instanceof Map) {
                    csf = Integer.valueOf(((Map) logData).get("CSF").toString());
                }

                if (csf != null) {
                    return csf.equals(1);
                } else {
                    return false;
                }
            } catch (Exception e) {
                return false;
            }
        }

        return false;
    }

    protected void enqueueRedoLogContent(RedoLogContent redoLogContent) {
        try {
            while (!logQueue.offer(redoLogContent, 1, TimeUnit.SECONDS)) {
            }
        } catch (InterruptedException ignore) {

        }
    }

    protected void processOrBuffRedoLogContent(RedoLogContent redoLogContent,
                                               Consumer<Map<String, OracleTransaction>> redoLogContentConsumer) {
        long scn = redoLogContent.getScn();
        String rsId = redoLogContent.getRsId();
        String xid = redoLogContent.getXid();
        String operation = redoLogContent.getOperation();

//        if (StringUtils.isNotBlank(redoLogContent.getXid())
//                && !StringUtils.equalsAnyIgnoreCase(redoLogContent.getOperation(), OracleConstant.REDO_LOG_OPERATION_COMMIT, OracleConstant.REDO_LOG_OPERATION_ROLLBACK)) {
//
//            // if current redo log content in large transaction waiting list, skip it
//            OracleTransaction oracleTransaction = transactionBucket.get(redoLogContent.getXid());
//            Optional.ofNullable(oracleTransaction).ifPresent(t -> t.incrementSize(1));
//            return;
//        }

        if (hasRollbackTemp) {
            rollbackTempHandle(transactionBucket);
            // 处理包含rollback + commit的事务需要等待提交的场景
            final List<String> oracleTransactions = commitTempHandle(transactionBucket, redoLogContent);
            if (EmptyKit.isNotEmpty(oracleTransactions)) {
                for (String waitingCommitXid : oracleTransactions) {
                    final OracleTransaction oracleTransaction = transactionBucket.get(waitingCommitXid);
                    if (oracleTransaction != null) {
                        TapLogger.info(TAG, "Delay commit transaction[scn: {}, xid: {}], redo size: {}",
                                oracleTransaction.getScn(), oracleTransaction.getXid(), oracleTransaction.getSize());
                        commitTransaction(redoLogContentConsumer, oracleTransaction);
                    }
                }
            }
        }

        switch (operation) {
            case OracleConstant.REDO_LOG_OPERATION_INSERT:
            case OracleConstant.REDO_LOG_OPERATION_UPDATE:
            case OracleConstant.REDO_LOG_OPERATION_DELETE:
            case OracleConstant.REDO_LOG_OPERATION_SELECT_FOR_UPDATE:
            case OracleConstant.REDO_LOG_OPERATION_LOB_TRIM:
            case OracleConstant.REDO_LOG_OPERATION_LOB_WRITE:
            case OracleConstant.REDO_LOG_OPERATION_SEL_LOB_LOCATOR:
                if (!transactionBucket.containsKey(xid)) {

                    TapLogger.debug(TAG, TapLog.D_CONN_LOG_0003.getMsg(), xid);
                    Map<String, List<RedoLogContent>> redoLogContents = new LinkedHashMap<>();
                    redoLogContents.put(rsId, new ArrayList<>(4));
                    redoLogContents.get(rsId).add(redoLogContent);
                    OracleTransaction orclTransaction = new OracleTransaction(rsId, scn, xid, redoLogContents, redoLogContent.getTimestamp().getTime());
                    setRacMinimalScn(orclTransaction);
                    orclTransaction.incrementSize(1);

                    if (OracleConstant.REDO_LOG_OPERATION_UPDATE.equals(redoLogContent.getOperation())) {
                        orclTransaction.getTxUpdatedRowIds().add(redoLogContent.getRowId());
                    }

                    transactionBucket.put(xid, orclTransaction);
                } else {
                    OracleTransaction oracleTransaction = transactionBucket.get(xid);
                    Map<String, List<RedoLogContent>> redoLogContents = oracleTransaction.getRedoLogContents();

                    try {
                        if (!needToAborted(operation, redoLogContent, redoLogContents)) {

                            // cache redo log event
                            oracleTransaction.addRedoLogContent(redoLogContent);

                            oracleTransaction.incrementSize(1);
                            long txLogContentsSize = oracleTransaction.getSize();
                            if (txLogContentsSize % OracleTransaction.LARGE_TRANSACTION_UPPER_LIMIT == 0) {
                                TapLogger.info(TAG, TapLog.CON_LOG_0008.getMsg(), xid, txLogContentsSize);
                            }
                        }
                    } catch (Exception e) {
                        TapLogger.error(TAG, e.getMessage());
                    }
                }

                break;
            case OracleConstant.REDO_LOG_OPERATION_COMMIT:
                if (transactionBucket.containsKey(xid)) {
                    OracleTransaction orclTransaction = transactionBucket.get(xid);

                    // 判断是否需要做等待提交
                    if (!need2WaitingCommit(orclTransaction)) {
                        // 提交事务
                        commitTransaction(redoLogContentConsumer, orclTransaction);
                    } else {
                        orclTransaction.setRollbackTemp(0);
                        orclTransaction.setLastTimestamp(redoLogContent.getTimestamp().getTime());
                        orclTransaction.setLastCommitTimestamp(redoLogContent.getCommitTimestamp().getTime());
                    }

                } else {
                    Map<String, List<RedoLogContent>> redoLogContents = new LinkedHashMap<>();
                    redoLogContents.put(rsId, new ArrayList<>(4));
                    redoLogContents.get(rsId).add(redoLogContent);
                    OracleTransaction orclTransaction = new OracleTransaction(rsId, scn, xid, redoLogContents);
                    setRacMinimalScn(orclTransaction);

                    orclTransaction.setTransactionType(OracleTransaction.TX_TYPE_COMMIT);

                    orclTransaction.incrementSize(1);

                    Map<String, OracleTransaction> cacheCommitTraction = new HashMap<>();
                    cacheCommitTraction.put(xid, orclTransaction);
                    redoLogContentConsumer.accept(cacheCommitTraction);
                }
                break;
            case OracleConstant.REDO_LOG_OPERATION_DDL:

                TapLogger.debug(TAG, TapLog.D_CONN_LOG_0003.getMsg(), xid);
                Map<String, List<RedoLogContent>> redoLogContents = new LinkedHashMap<>();
                redoLogContents.put(rsId, new ArrayList<>(4));
                redoLogContents.get(rsId).add(redoLogContent);
                OracleTransaction orclTransaction = new OracleTransaction(rsId, scn, xid, redoLogContents);
                setRacMinimalScn(orclTransaction);

                orclTransaction.setTransactionType(OracleTransaction.TX_TYPE_DDL);
                Map<String, OracleTransaction> cacheCommitTraction = new HashMap<>();
                cacheCommitTraction.put(xid, orclTransaction);
                redoLogContentConsumer.accept(cacheCommitTraction);

                break;
            case OracleConstant.REDO_LOG_OPERATION_ROLLBACK:
                if (transactionBucket.containsKey(xid)) {
                    OracleTransaction oracleTransaction = transactionBucket.get(xid);
                    if (oracleTransaction.isLarge()) {
                        TapLogger.info(TAG, "Found large transaction be rolled back: {}", oracleTransaction);
                    }

                    /**
                     * 先不删除事务，防止该rollback是无效的, 参考方法描述{@link AbstractRedoLogMiner#rollbackTempHandle(java.util.LinkedHashMap)}
                     */
                    hasRollbackTemp = true;
                    oracleTransaction.setRollbackTemp(1);
                    oracleTransaction.setHasRollback(true);
                }
                break;
            default:
                break;
        }
    }

    private void sendTransaction(Map<String, OracleTransaction> txMap) {
        for (Map.Entry<String, OracleTransaction> txEntry : txMap.entrySet()) {
            OracleTransaction oracleTransaction = txEntry.getValue();
//            String transactionType = oracleTransaction.getTransactionType();
            List<TapEvent> eventList = TapSimplify.list();
            Map<String, List<RedoLogContent>> redoLogContents = oracleTransaction.getRedoLogContents();
            RedoLogContent redoLogContent = null;
            for (List<RedoLogContent> redoLogContentList : redoLogContents.values()) {
                for (RedoLogContent txRedoLogContent : redoLogContentList) {
                    redoLogContent = txRedoLogContent;
                }
            }
            switch (Objects.requireNonNull(redoLogContent).getOperation()) {
                case "INSERT":
                    eventList.add(new TapInsertRecordEvent()
                            .table(redoLogContent.getTableName())
                            .after(redoLogContent.getRedoRecord())
                            .referenceTime(redoLogContent.getTimestamp().getTime()));
                    break;
                case "UPDATE":
                    eventList.add(new TapUpdateRecordEvent()
                            .table(redoLogContent.getTableName())
                            .after(redoLogContent.getRedoRecord())
                            .referenceTime(redoLogContent.getTimestamp().getTime()));
                    break;
                case "DELETE":
                    eventList.add(new TapDeleteRecordEvent()
                            .table(redoLogContent.getTableName())
                            .before(redoLogContent.getRedoRecord())
                            .referenceTime(redoLogContent.getTimestamp().getTime()));
                    break;
                default:
                    break;
            }
            OracleOffset oracleOffset = new OracleOffset();
            oracleOffset.setLastScn(redoLogContent.getScn());
            oracleOffset.setPendingScn(redoLogContent.getScn());
            if (eventList.size() > 0) {
                consumer.accept(eventList, oracleOffset);
            }
//            if (OracleTransaction.TX_TYPE_COMMIT.equals(transactionType)) {
//
//                continue;
//            }
//            oracleTransaction.setRedoLogContents(LobWriteUtil.filterAndAddTag(oracleTransaction.getRedoLogContents()));
//            Map<String, List<RedoLogContent>> redoLogContentMap = oracleTransaction.getRedoLogContents();
            // TODO: 2022/6/22 非commit事务逻辑
        }
    }

    private void rollbackTempHandle(LinkedHashMap<String, OracleTransaction> transactionBucket) {
        if (EmptyKit.isEmpty(transactionBucket)) {
            return;
        }
        Iterator<String> iterator = transactionBucket.keySet().iterator();
        hasRollbackTemp = false;
        while (iterator.hasNext()) {
            String bucketXid = iterator.next();
            OracleTransaction bucketTransaction = transactionBucket.get(bucketXid);
            int rollbackTemp = bucketTransaction.getRollbackTemp();
            if (bucketTransaction.isHasRollback()) {
                hasRollbackTemp = true;
            }
            if (rollbackTemp <= 0) {
                continue;
            }
            if (rollbackTemp < ROLLBACK_TEMP_LIMIT) {
                // 计数+1
                bucketTransaction.setRollbackTemp(++rollbackTemp);
                hasRollbackTemp = true;
            } else {
                TapLogger.info(TAG, "It was found that the transaction[first scn: {}, xid: {}] that was rolled back did not commit after {} events, " +
                        "and the modification of this transaction was truly discarded", bucketTransaction.getScn(), bucketXid, ROLLBACK_TEMP_LIMIT);
                iterator.remove();
            }
        }
    }

    private List<String> commitTempHandle(LinkedHashMap<String, OracleTransaction> transactionBucket, RedoLogContent redoLogContent) {
        List<String> need2CommitTxs = new ArrayList<>();
        if (EmptyKit.isNotEmpty(transactionBucket)) {
            transactionBucket.values().stream().forEach(oracleTransaction -> {
                if (oracleTransaction == null || oracleTransaction.getLastTimestamp() == null || oracleTransaction.getLastCommitTimestamp() == null
                        || redoLogContent.getTimestamp() == null || redoLogContent.getCommitTimestamp() == null) {
                    return;
                }
                /**
                 * 2021-04-13
                 * rollback+commit的事务，如果timestamp, commitTimestamp小于当前事件的timestamp, commitTimestamp
                 * 则提交该事务
                 */
                if (oracleTransaction.getLastTimestamp().compareTo(redoLogContent.getTimestamp().getTime()) < 0 ||
                        oracleTransaction.getLastCommitTimestamp().compareTo(redoLogContent.getCommitTimestamp().getTime()) < 0) {
                    need2CommitTxs.add(oracleTransaction.getXid());
                }
            });
        }

        return need2CommitTxs;
    }

    private void commitTransaction(Consumer<Map<String, OracleTransaction>> redoLogContentConsumer, OracleTransaction orclTransaction) {
        final String xid = orclTransaction.getXid();
        transactionBucket.remove(xid);
        long txLogContentsSize = orclTransaction.getSize();
        if (orclTransaction.isHasRollback()) {
            TapLogger.info(TAG, "Found commit that had a rollback before it, first scn: {}, xid: {}, log content size: {}", orclTransaction.getScn(), xid, txLogContentsSize);
        }

        if (txLogContentsSize >= OracleTransaction.LARGE_TRANSACTION_UPPER_LIMIT) {
            TapLogger.info(TAG, TapLog.D_CONN_LOG_0002.getMsg(), xid, txLogContentsSize);
        } else {
            TapLogger.debug(TAG, TapLog.D_CONN_LOG_0002.getMsg(), xid, txLogContentsSize);
        }

        Map<String, OracleTransaction> cacheCommitTraction = new HashMap<>();
        cacheCommitTraction.put(xid, orclTransaction);
        redoLogContentConsumer.accept(cacheCommitTraction);
    }

    private void setRacMinimalScn(OracleTransaction oracleTransaction) {
        if (EmptyKit.isNotEmpty(instancesMindedScnMap) && instancesMindedScnMap.size() > 1) {
            long racMinimalSCN = 0L;
            for (Long mindedSCN : instancesMindedScnMap.values()) {
                racMinimalSCN = racMinimalSCN < mindedSCN ? racMinimalSCN : mindedSCN;
            }

            oracleTransaction.setRacMinimalScn(racMinimalSCN);
        }
    }

    private boolean needToAborted(String operation, RedoLogContent redoLogContent, Map<String, List<RedoLogContent>> redoLogContents) {
        boolean needToAborted = false;

        if (StringUtils.isNotBlank(redoLogContent.getSqlUndo()) || EmptyKit.isNotEmpty(redoLogContent.getRedoRecord())) {
            return false;
        }

        String rowId = redoLogContent.getRowId();
        if (OracleConstant.REDO_LOG_OPERATION_DELETE.equals(operation)) {

            Iterator<String> keySetIter = redoLogContents.keySet().iterator();
            while (keySetIter.hasNext()) {
                String key = keySetIter.next();
                List<RedoLogContent> logContents = redoLogContents.get(key);

                Iterator<RedoLogContent> iterator = logContents.iterator();
                while (iterator.hasNext()) {
                    RedoLogContent logContent = iterator.next();

                    if (OracleConstant.REDO_LOG_OPERATION_INSERT.equals(logContent.getOperation())) {
                        String insertedRowId = logContent.getRowId();
                        if (insertedRowId.equals(rowId)) {
                            TapLogger.info("Found insert row was deleted by row id {} on the same transaction, insert event {}, delete event {}", rowId, logContent, redoLogContent);
                            iterator.remove();
                            needToAborted = true;
                        }
                    }
                }

                if (needToAborted && EmptyKit.isEmpty(logContents)) {
                    keySetIter.remove();
                }
            }
        } else if (OracleConstant.REDO_LOG_OPERATION_UPDATE.equals(operation)) {
            try {
                String currentBetweenSetAndWhere = StringKit.subStringBetweenTwoString(redoLogContent.getSqlRedo(), "set", "where");

                if (StringUtils.isBlank(currentBetweenSetAndWhere) && EmptyKit.isEmpty(redoLogContent.getRedoRecord())) {
                    return true;
                }

                Iterator<String> keyIter = redoLogContents.keySet().iterator();

                while (keyIter.hasNext() && !needToAborted) {
                    List<RedoLogContent> logContents = redoLogContents.get(keyIter.next());

                    Iterator<RedoLogContent> iterator = logContents.iterator();

                    while (iterator.hasNext()) {
                        RedoLogContent logContent = iterator.next();
                        if (!OracleConstant.REDO_LOG_OPERATION_UPDATE.equals(logContent.getOperation()) || !rowId.equals(logContent.getRowId())) {
                            continue;
                        }
                        String betweenSetAndWhere = StringKit.subStringBetweenTwoString(logContent.getSqlUndo(), "set", "where");

                        if (currentBetweenSetAndWhere.equals(betweenSetAndWhere)) {
                            needToAborted = true;
                        } else if (redoLogContent.getRollback() == 1 && StringUtils.indexOf(betweenSetAndWhere, currentBetweenSetAndWhere.trim()) > -1) {
                            needToAborted = true;
                        }

                        if (needToAborted) {
                            TapLogger.debug(TAG, "Found update row was undo updated by row id {} on the same transaction, update event {}, undo update event {}", rowId, logContent, redoLogContent);
                            iterator.remove();
                            break;
                        }
                    }

                    if (needToAborted && EmptyKit.isEmpty(logContents)) {
                        keyIter.remove();
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(String.format("Check abort update oracle log failed, err: %s, scn: %s, xid: %s, timestamp: %s",
                        e.getMessage(), redoLogContent.getScn(), redoLogContent.getXid(), redoLogContent.getTimestamp()), e);
            }
        } else if (OracleConstant.REDO_LOG_OPERATION_INSERT.equals(operation)) {

            if (StringUtils.isBlank(redoLogContent.getSqlRedo()) && EmptyKit.isEmpty(redoLogContent.getRedoRecord())) {
                return true;
            }

            Iterator<String> keyIter = redoLogContents.keySet().iterator();

            while (keyIter.hasNext()) {
                List<RedoLogContent> logContents = redoLogContents.get(keyIter.next());

                Iterator<RedoLogContent> iterator = logContents.iterator();

                while (iterator.hasNext()) {
                    RedoLogContent logContent = iterator.next();
                    if (!OracleConstant.REDO_LOG_OPERATION_DELETE.equals(logContent.getOperation())) {
                        continue;
                    }
                    if (StringUtils.isBlank(logContent.getSqlRedo())) {
                        continue;
                    }

                    if (rowId.equals(logContent.getRowId())
                            && redoLogContent.getSqlRedo().equals(logContent.getSqlUndo())
                    ) {
                        TapLogger.info(TAG, "Found delete row was undo inserted by row id {} on the same transaction, delete event {}, undo insert event {}", rowId, logContent, redoLogContent);
                        iterator.remove();
                        needToAborted = true;
                    }
                }

                if (needToAborted && EmptyKit.isEmpty(logContents)) {
                    keyIter.remove();
                }
            }
        }
        return needToAborted;
    }

    private boolean need2WaitingCommit(OracleTransaction transaction) {
        transaction.setReceivedCommitTs(System.currentTimeMillis());
        return transaction.isHasRollback();
    }

    private boolean canParse(RedoLogContent redoLogContent) {
        if (redoLogContent == null) {
            return false;
        }
        if (redoLogContent.getUndoRecord() != null || redoLogContent.getRedoRecord() != null) {
            return false;
        }

        switch (redoLogContent.getOperation()) {
            // lob类型无法预解析
            case OracleConstant.REDO_LOG_OPERATION_LOB_TRIM:
            case OracleConstant.REDO_LOG_OPERATION_LOB_WRITE:
            case OracleConstant.REDO_LOG_OPERATION_SEL_LOB_LOCATOR:
            case "INTERNAL": // 无法解析
                return false;
            default:
                break;
        }

        String sqlRedo = redoLogContent.getSqlRedo();
        String sqlUndo = redoLogContent.getSqlUndo();
        if (StringUtils.isAllBlank(sqlRedo, sqlUndo)) {
            return false;
        }

        String operation = redoLogContent.getOperation();
        if (!StringUtils.equalsAny(operation,
                OracleConstant.REDO_LOG_OPERATION_INSERT,
                OracleConstant.REDO_LOG_OPERATION_UPDATE,
                OracleConstant.REDO_LOG_OPERATION_DELETE)) {
            return false;
        }
        //回滚的delete事件undo为空，后面的解析会报错，需要在预解析中排除
        return !StringUtils.equalsAny(operation, OracleConstant.REDO_LOG_OPERATION_DELETE)
                || !StringUtils.isEmpty(redoLogContent.getSqlUndo());
    }

    private void convertStringToObject(RedoLogContent redoLogContent) {
        for (Map.Entry<String, Object> stringObjectEntry : redoLogContent.getRedoRecord().entrySet()) {
            Object value = stringObjectEntry.getValue();
            String table = redoLogContent.getTableName();
            String column = stringObjectEntry.getKey();
            int columnType = columnTypeMap.get(table + "." + column);
            switch (columnType) {
                case Types.BIGINT:
                    stringObjectEntry.setValue(new BigDecimal((String) value).longValue());
                    break;
                case Types.BINARY:
                case Types.LONGVARBINARY:
                case Types.VARBINARY:
                    try {
                        stringObjectEntry.setValue(RawTypeHandler.parseRaw((String) value));
                    } catch (DecoderException e) {
                        TapLogger.warn(TAG, TapLog.W_CONN_LOG_0014.getMsg(), value, columnType, e.getMessage());
                    }
                    break;
                case Types.BIT:
                case Types.BOOLEAN:
                    stringObjectEntry.setValue(Boolean.valueOf((String) value));
                    break;
                case Types.CHAR:
                case Types.LONGNVARCHAR:
                case Types.LONGVARCHAR:
                case Types.VARCHAR:
                    break;
                case Types.NCHAR:
                case Types.NVARCHAR:
                    stringObjectEntry.setValue(UnicodeStringColumnHandler.getUnicdeoString((String) value));
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                case Types.DOUBLE:
                    stringObjectEntry.setValue(new BigDecimal((String) value).doubleValue());
                    break;
                case Types.FLOAT:
                case Types.REAL:
                    stringObjectEntry.setValue(new BigDecimal((String) value).floatValue());
                    break;
                case Types.INTEGER:
                    stringObjectEntry.setValue(new BigDecimal((String) value).intValue());
                    break;
                case Types.SMALLINT:
                case Types.TINYINT:
                    stringObjectEntry.setValue(new BigDecimal((String) value).shortValue());
                    break;
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    String actualType = dateTimeTypeMap.get(table + "." + column);
                    if (StringUtils.contains((CharSequence) value, "::")) {
                        stringObjectEntry.setValue(dateTimeColumnHandler.getTimestamp(value, actualType));
                    } else {
                        // For whatever reason, Oracle returns all the date/time/timestamp fields as the same type, so additional
                        // logic is required to accurately parse the type
                        stringObjectEntry.setValue(dateTimeColumnHandler.getDateTimeStampField((String) value, actualType));
                    }
                    break;
                case Types.TIMESTAMP_WITH_TIMEZONE:
                case TIMESTAMP_TZ_TYPE:
                    String tzDateType = dateTimeTypeMap.get(table + "." + column);
                    if (StringUtils.contains((CharSequence) value, "::")) {
                        stringObjectEntry.setValue(dateTimeColumnHandler.getTimestamp(value, tzDateType));
                    } else {
                        stringObjectEntry.setValue(dateTimeColumnHandler.getTimestampWithTimezoneField((String) value, tzDateType));
                    }
                    break;
                case TIMESTAMP_LTZ_TYPE:
                    String ltzDateType = dateTimeTypeMap.get(table + "." + column);
                    if (StringUtils.contains((CharSequence) value, "::")) {
                        stringObjectEntry.setValue(dateTimeColumnHandler.getTimestamp(value, ltzDateType));
                    } else {
                        stringObjectEntry.setValue(dateTimeColumnHandler.getTimestampWithLocalTimezone((String) value, ltzDateType));
                    }
                    break;
                case Types.BLOB:
                case Types.CLOB:
                case Types.NCLOB:
                    if ("EMPTY_BLOB()".equals(value)) {
                        stringObjectEntry.setValue(null);
                    }
                    break;
                case Types.ROWID:
                case Types.ARRAY:
                case Types.DATALINK:
                case Types.DISTINCT:
                case Types.JAVA_OBJECT:
                case Types.NULL:
                case Types.OTHER:
                case Types.REF:
                case Types.REF_CURSOR:
                case Types.SQLXML:
                case Types.STRUCT:
                case Types.TIME_WITH_TIMEZONE:
                    break;
            }
        }
    }
}
