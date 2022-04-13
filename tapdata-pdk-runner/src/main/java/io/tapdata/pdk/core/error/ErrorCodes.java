package io.tapdata.pdk.core.error;

public interface ErrorCodes {
    int COMMON_ILLEGAL_PARAMETERS = 10000;
    int COMMON_UNKNOWN = 10001;
    int COMMON_SINGLE_THREAD_QUEUE_STOPPED = 10002;
    int COMMON_SINGLE_THREAD_BLOCKING_QUEUE_NO_EXECUTOR = 10003;

    int MAIN_DAG_IS_ILLEGAL = 20000;
    int MAIN_CONNECTOR_CLASS_INITIATE_FAILED = 20001;
    int MAIN_CONNECTOR_CLASS_INITIATE_UNKNOWN_ERROR = 20002;
    int MAIN_SOURCE_NODES_EMPTY_WHILE_START_DATAFLOW = 20003;
    int MAIN_START_DATAFLOW_FAILED = 20004;
    int MAIN_DATASOURCE_IS_ILLEGAL = 20005;
    int MAIN_CONNECTOR_CLASS_IS_NULL = 20006;
    int MAIN_DAG_WORKER_STARTED_ALREADY = 20007;
    int MAIN_DATAFLOW_NOT_FOUND = 20008;
    int MAIN_DATAFLOW_WORKER_ILLEGAL_STATE = 20009;
    int MAIN_DATANODE_INIT_ILLEGAL = 20010;
    int MAIN_NODE_OPTIONS_VERIFY_ILLEGAL = 20011;


    int CLI_MISSING_SOURCE_OR_TARGET = 30000;
    int CLI_SOURCE_NODE_MISSING_DATA_SOURCE_OR_TABLE = 30001;
    int CLI_TARGET_NODE_MISSING_CONNECTION_STRING_OR_TYPE = 30002;
    int CLI_TARGET_NODE_MISSING_DATABASE_OR_NAME = 30003;
    int CLI_TARGET_NODE_MISSING_DATA_SOURCE_OR_TABLE = 30004;
    int CLI_SOURCE_NODE_MISSING_DATABASE_OR_NAME = 30005;
    int CLI_SOURCE_NODE_MISSING_CONNECTION_STRING_OR_TYPE = 30006;
    int CLI_SCRIPT_NODE_MISSING_CLASS_NAME_OR_ROOT_PATH = 30007;
    int CLI_JAVA_VERSION_ILLEGAL = 30008;

    int IMPL_CREATE_FAILED = 40000;
    int IMPL_CREATE_TYPE_FAILED = 40001;

    int NODE_CREATE_OPENAPI_CONNECTOR = 50000;
    int NODE_CREATE_CONNECTOR_NOT_EXISTS = 50001;
    int NODE_CREATE_PROCESSOR_NOT_EXISTS = 50002;
    int NODE_TYPE_ILLEGAL = 50003;

    int PDK_ILLEGAL_PARAMETER = 60000;
    int PDK_PROCESSOR_NOTFOUND = 60001;
    int PDK_TARGET_NOTFOUND = 60002;

    int TDD_READ_TEST_CONFIG_FAILED = 70000;
    int TDD_TAPNODEINFO_NOT_FOUND = 70001;
    int TDD_TEST_FAILED = 70002;
    int TDD_FORCE_QUIT = 70003;

    int SOURCE_MISSING_FIELDS_IN_TABLE = 80000;
    int SOURCE_EXCEEDED_BATCH_SIZE = 80001;
}
