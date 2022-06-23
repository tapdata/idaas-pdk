package io.tapdata.connector.oracle.cdc.logminer;

public class OracleSqlConstant {

    static final String STORE_DICT_IN_REDO_SQL = "BEGIN\n" +
            "  SYS.DBMS_LOGMNR_D.BUILD(OPTIONS=> SYS.DBMS_LOGMNR_D.STORE_IN_REDO_LOGS);\n" +
            "END;";
    static final String LAST_DICT_ARCHIVE_LOG_BY_SCN = "\n" +
            "SELECT\n" +
            "  NAME,\n" +
            "  SEQUENCE#,\n" +
            "  DICTIONARY_BEGIN d_beg,\n" +
            "  DICTIONARY_END   d_end,\n" +
            "  first_change#,\n" +
            "  next_change#\n" +
            "FROM V$ARCHIVED_LOG\n" +
            "WHERE SEQUENCE# = (SELECT MAX(SEQUENCE#)\n" +
            "                   FROM V$ARCHIVED_LOG\n" +
            "                   WHERE DICTIONARY_END = 'YES' AND next_change# > %s)";
    static final String ADD_REDO_LOG_FILE_FOR_LOGMINER = "BEGIN SYS.dbms_logmnr.add_logfile(\n" +
            "    logfilename=>'%s',\n" +
            "    options=>SYS.dbms_logmnr.NEW);\n" +
            "END;";
    static final String CHECK_CURRENT_SCN = "SELECT current_scn FROM V$DATABASE";
    static final String CHECK_CURRENT_SCN_9I = "SELECT dbms_flashback.get_system_change_number as current_scn FROM DUAL";
    static final String CHECK_ARCHIVED_LOG_EXISTS = "SELECT count(1)\n" +
            "      FROM v$archived_log WHERE STATUS = 'A'";

    static final String GET_FIRST_ONLINE_REDO_LOG_FILE_FOR_10G_AND_9I = "SELECT t.*\n" +
            "FROM (\n" +
            "       SELECT\n" +
            "         lf.MEMBER               NAME,\n" +
            "         l.FIRST_TIME,\n" +
            "         l.FIRST_CHANGE#,\n" +
            "         l.STATUS                status,\n" +
            "         l.sequence#,\n" +
            "         (l.bytes / 1024 / 1024) SIZEINMB,\n" +
            "         l.THREAD#\n" +
            "       FROM v$log l LEFT JOIN v$logfile lf ON l.GROUP# = lf.GROUP#\n" +
            "       WHERE  l.STATUS = 'ACTIVE' OR l.STATUS = 'CURRENT'\n" +
            "       ORDER BY l.FIRST_CHANGE#) t\n" +
            "WHERE ROWNUM <= 1\n";

    static final String GET_FIRST_ONLINE_REDO_LOG_FILE_BY_SCN_FOR_10G_AND_9I = "SELECT t.*\n" +
            "FROM (\n" +
            "       SELECT\n" +
            "         lf.MEMBER               NAME,\n" +
            "         l.FIRST_TIME,\n" +
            "         l.FIRST_CHANGE#,\n" +
            "         l.STATUS                status,\n" +
            "         l.sequence#,\n" +
            "         (l.bytes / 1024 / 1024) SIZEINMB,\n" +
            "         l.THREAD#\n" +
            "       FROM v$log l LEFT JOIN v$logfile lf ON l.GROUP# = lf.GROUP#\n" +
            "       WHERE (l.STATUS = 'ACTIVE' OR l.STATUS = 'CURRENT' ) AND l.FIRST_CHANGE# <= %s\n" +
            "       ORDER BY l.FIRST_CHANGE# DESC) t\n" +
            "WHERE ROWNUM <= 1\n";

    static final String GET_FIRST_ONLINE_REDO_LOG_FILE = "SELECT t.*\n" +
            "FROM (\n" +
            "       SELECT\n" +
            "         lf.MEMBER               NAME,\n" +
            "         l.FIRST_TIME,\n" +
            "         l.FIRST_CHANGE#,\n" +
            "         l.STATUS                status,\n" +
            "         l.sequence#,\n" +
            "         (l.bytes / 1024 / 1024) SIZEINMB,\n" +
            "         l.THREAD#,\n" +
            "         l.NEXT_CHANGE#,\n" +
            "         l.NEXT_TIME\n" +
            "       FROM v$log l LEFT JOIN v$logfile lf ON l.GROUP# = lf.GROUP#\n" +
            "       WHERE  l.STATUS = 'ACTIVE' OR l.STATUS = 'CURRENT'\n" +
            "       ORDER BY l.FIRST_CHANGE#) t\n" +
            "WHERE ROWNUM <= 1\n";

    static final String GET_FIRST_ONLINE_REDO_LOG_FILE_BY_SCN = "SELECT t.*\n" +
            "FROM (\n" +
            "       SELECT\n" +
            "         lf.MEMBER               NAME,\n" +
            "         l.FIRST_TIME,\n" +
            "         l.FIRST_CHANGE#,\n" +
            "         l.STATUS                status,\n" +
            "         l.sequence#,\n" +
            "         (l.bytes / 1024 / 1024) SIZEINMB,\n" +
            "         l.THREAD#,\n" +
            "         l.NEXT_CHANGE#,\n" +
            "         l.NEXT_TIME\n" +
            "       FROM v$log l LEFT JOIN v$logfile lf ON l.GROUP# = lf.GROUP#\n" +
            "       WHERE (l.STATUS = 'ACTIVE' OR l.STATUS = 'CURRENT' ) AND l.FIRST_CHANGE# <= %s\n" +
            "       ORDER BY l.FIRST_CHANGE# DESC) t\n" +
            "WHERE ROWNUM <= 1\n";

    static final String START_LOG_MINOR_CONTINUOUS_MINER_SQL = "BEGIN\n" +
            "  SYS.DBMS_LOGMNR.START_LOGMNR(" +
            "                               STARTSCN => %s,\n" +
            "                               OPTIONS => SYS.DBMS_LOGMNR.DDL_DICT_TRACKING +\n" +
            "                                          SYS.DBMS_LOGMNR.DICT_FROM_REDO_LOGS +\n" +
            "                                          SYS.DBMS_LOGMNR.CONTINUOUS_MINE +\n" +
            "                                          SYS.DBMS_LOGMNR.NO_SQL_DELIMITER\n" +
            "  );\n" +
            "END;";

    static final String END_LOG_MINOR_SQL = "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;";

    static final String GET_REDO_LOG_RESULT_ORACLE_LOG_COLLECT_SQL = "SELECT SCN,OPERATION,TIMESTAMP,STATUS,SQL_REDO,SQL_UNDO,ROW_ID,TABLE_NAME,RS_ID,SSN," +
            "(XIDUSN || '.' || XIDSLT || '.' || XIDSQN) AS XID, OPERATION_CODE, SEG_OWNER, CSF, ROLLBACK, THREAD#, COMMIT_TIMESTAMP, INFO FROM V$LOGMNR_CONTENTS" +
            " WHERE %s operation != 'SELECT_FOR_UPDATE' AND SCN > %s AND (operation = 'COMMIT' OR (operation = 'ROLLBACK' AND PXID != '0000000000000000') OR (SEG_TYPE IN (2, 19) %s %s))";

    static final String GET_REDO_LOG_RESULT_ORACLE_LOG_COLLECT_SQL_9i = "SELECT SCN,OPERATION,TIMESTAMP,STATUS,SQL_REDO,SQL_UNDO,ROW_ID,DATA_OBJ#,RS_ID,SSN," +
            "(XIDUSN || '.' || XIDSLT || '.' || XIDSQN) AS XID, OPERATION_CODE, SEG_OWNER, CSF, ROLLBACK, COMMIT_TIMESTAMP, INFO FROM V$LOGMNR_CONTENTS" +
            " WHERE operation != 'SELECT_FOR_UPDATE' AND SCN > %s AND (operation = 'COMMIT' OR (operation = 'ROLLBACK' AND PXID != '0000000000000000') OR (SEG_TYPE IN (2, 19) %s %s))";

    static final String SWITCH_TO_CDB_ROOT = "ALTER SESSION SET CONTAINER = CDB$ROOT";

    static final String GET_TABLE_OBJECT_ID_WITH_CLAUSE = "SELECT OBJECT_NAME, OBJECT_ID\n" +
            "FROM ALL_OBJECTS WHERE OBJECT_TYPE='TABLE' AND OWNER IN (%s) AND OBJECT_NAME IN (%s)";
}
