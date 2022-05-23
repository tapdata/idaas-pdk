package io.tapdata.pdk.core.api;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.TapConnectorNode;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.apis.TapProcessor;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.context.TapProcessorContext;
import io.tapdata.pdk.apis.functions.ProcessorFunctions;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.dag.TapDAGNode;
import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.pdk.core.tapnode.TapNodeInstance;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

public class PDKIntegration {
    private static TapConnectorManager tapConnectorManager;

    private static final String TAG = PDKIntegration.class.getSimpleName();

    private PDKIntegration() {}

    public abstract static class ConnectionBuilder<T extends Node> {
        protected String associateId;
        protected DataMap connectionConfig;
        protected String pdkId;
        protected String group;
        protected String version;

        public String verify() {
            if(associateId == null)
                return "missing associateId";
            if(pdkId == null)
                return "missing pdkId";
            if(group == null)
                return "missing group";
            if(version == null)
                return "missing version";
            return null;
        }

        /**
         * Each
         * @param associateId
         * @return
         */
        public ConnectionBuilder<T> withAssociateId(String associateId) {
            this.associateId = associateId;
            return this;
        }

        public ConnectionBuilder<T> withPdkId(String pdkId) {
            this.pdkId = pdkId;
            return this;
        }

        public ConnectionBuilder<T> withGroup(String group) {
            this.group = group;
            return this;
        }

        public ConnectionBuilder<T> withVersion(String version) {
            this.version = version;
            return this;
        }

        public ConnectionBuilder<T> withConnectionConfig(DataMap connectionConfig) {
            this.connectionConfig = connectionConfig;
            return this;
        }

        public ConnectionBuilder<T> withTapDAGNode(TapDAGNode node) {
            this.pdkId = node.getPdkId();
            this.group = node.getGroup();
            this.version = node.getVersion();
            this.connectionConfig = node.getConnectionConfig();
            this.associateId = node.getId();
            return this;
        }

        protected void checkParams() {
            String result = verify();
            if(result != null)
                throw new CoreException(PDKRunnerErrorCodes.PDK_ILLEGAL_PARAMETER, "Illegal parameter, " + result);
        }

        public abstract T build();
    }

    public abstract static class ProcessorBuilder<T extends Node> {
        protected DataMap nodeConfig;
        protected String dagId;

        protected String associateId;
        protected DataMap connectionConfig;
        protected String pdkId;
        protected String group;
        protected String version;

        public String verify() {
            if(associateId == null)
                return "missing associateId";
            if(pdkId == null)
                return "missing pdkId";
            if(group == null)
                return "missing group";
            if(version == null)
                return "missing version";
            if(dagId == null)
                return "missing dagId";
            return null;
        }

        /**
         * Each
         * @param associateId
         * @return
         */
        public ProcessorBuilder<T> withAssociateId(String associateId) {
            this.associateId = associateId;
            return this;
        }

        public ProcessorBuilder<T> withPdkId(String pdkId) {
            this.pdkId = pdkId;
            return this;
        }

        public ProcessorBuilder<T> withGroup(String group) {
            this.group = group;
            return this;
        }

        public ProcessorBuilder<T> withVersion(String version) {
            this.version = version;
            return this;
        }

        public ProcessorBuilder<T> withConnectionConfig(DataMap connectionConfig) {
            this.connectionConfig = connectionConfig;
            return this;
        }

        public ProcessorBuilder<T> withTapDAGNode(TapDAGNode node) {
            this.pdkId = node.getPdkId();
            this.group = node.getGroup();
            this.version = node.getVersion();
            this.connectionConfig = node.getConnectionConfig();
            this.associateId = node.getId();
            this.nodeConfig = node.getNodeConfig();
            return this;
        }


        public ProcessorBuilder<T> withDagId(String dagId) {
            this.dagId = dagId;
            return this;
        }

        public ProcessorBuilder<T> withNodeConfig(DataMap nodeConfig) {
            this.nodeConfig = nodeConfig;
            return this;
        }

        protected void checkParams() {
            String result = verify();
            if(result != null)
                throw new CoreException(PDKRunnerErrorCodes.PDK_ILLEGAL_PARAMETER, "Illegal parameter, " + result);
        }

        public abstract T build();
    }

    public abstract static class ConnectorBuilder<T extends Node> {
        protected DataMap nodeConfig;
        protected String dagId;
        protected String associateId;
        protected DataMap connectionConfig;
        protected String pdkId;
        protected String group;
        protected String version;
        protected List<Map<String, Object>> tasks;
        protected String table;
        protected List<String> tables;
        protected KVReadOnlyMap<TapTable> tableMap;
        protected KVMap<Object> stateMap;

        public String verify() {
            if(associateId == null)
                return "missing associateId";
            if(pdkId == null)
                return "missing pdkId";
            if(group == null)
                return "missing group";
            if(version == null)
                return "missing version";
            if(dagId == null)
                return "missing dagId";
            /*if((tables == null || tables.isEmpty()) && table == null)
                return "missing tables or table";*/
            if(tableMap == null)
                return "missing tableMap";
            if(stateMap == null)
                return "missing stateMap";
            return null;
        }

        /**
         * Each
         * @param associateId
         * @return
         */
        public ConnectorBuilder<T> withAssociateId(String associateId) {
            this.associateId = associateId;
            return this;
        }

        public ConnectorBuilder<T> withPdkId(String pdkId) {
            this.pdkId = pdkId;
            return this;
        }

        public ConnectorBuilder<T> withGroup(String group) {
            this.group = group;
            return this;
        }

        public ConnectorBuilder<T> withTableMap(KVReadOnlyMap<TapTable> tableMap) {
            this.tableMap = tableMap;
            return this;
        }

        public ConnectorBuilder<T> withStateMap(KVMap<Object> stateMap) {
            this.stateMap = stateMap;
            return this;
        }

        public ConnectorBuilder<T> withVersion(String version) {
            this.version = version;
            return this;
        }

        public ConnectorBuilder<T> withConnectionConfig(DataMap connectionConfig) {
            this.connectionConfig = connectionConfig;
            return this;
        }

        public ConnectorBuilder<T> withTapDAGNode(TapDAGNode node) {
            this.pdkId = node.getPdkId();
            this.group = node.getGroup();
            this.version = node.getVersion();
            this.connectionConfig = node.getConnectionConfig();
            this.associateId = node.getId();
            this.nodeConfig = node.getNodeConfig();
            this.tasks = node.getTasks();
            this.table = node.getTable();
            this.tables = node.getTables();
            KVMapFactory mapFactory = InstanceFactory.instance(KVMapFactory.class);
            mapFactory.getCacheMap("tableMap_" + this.associateId, TapTable.class);
            this.tableMap = mapFactory.createKVReadOnlyMap("tableMap_" + this.associateId);
            this.stateMap = mapFactory.getPersistentMap("stateMap_" + this.associateId, Object.class);
            return this;
        }

        public ConnectorBuilder<T> withTasks(List<Map<String, Object>> tasks) {
            this.tasks = tasks;
            return this;
        }

        public ConnectorBuilder<T> withTable(String table) {
            this.table = table;
            return this;
        }

        public ConnectorBuilder<T> withTables(List<String> tables) {
            this.tables = tables;
            return this;
        }

        public ConnectorBuilder<T> withDagId(String dagId) {
            this.dagId = dagId;
            return this;
        }

        public ConnectorBuilder<T> withNodeConfig(DataMap nodeConfig) {
            this.nodeConfig = nodeConfig;
            return this;
        }

        protected void checkParams() {
            String result = verify();
            if(result != null)
                throw new CoreException(PDKRunnerErrorCodes.PDK_ILLEGAL_PARAMETER, "Illegal parameter, " + result);
        }

        public abstract T build();
    }

    public static class ConnectionConnectorBuilder extends ConnectionBuilder<ConnectionNode> {
        public ConnectionNode build() {
            checkParams();
            TapNodeInstance nodeInstance = TapConnectorManager.getInstance().createConnectorInstance(associateId, pdkId, group, version);
            if(nodeInstance == null)
                throw new CoreException(PDKRunnerErrorCodes.PDK_PROCESSOR_NOTFOUND, MessageFormat.format("Source not found for pdkId {0} group {1} version {2} for associateId {3}", pdkId, group, version, associateId));
            ConnectionNode connectionNode = new ConnectionNode();
            connectionNode.connectorNode = (TapConnectorNode) nodeInstance.getTapNode();
            connectionNode.associateId = associateId;
            connectionNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            connectionNode.connectionContext = new TapConnectionContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), connectionConfig);
            return connectionNode;
        }
    }

    public static class SourceConnectorBuilder extends ConnectorBuilder<SourceNode> {
        public SourceNode build() {
            checkParams();
            TapNodeInstance nodeInstance = TapConnectorManager.getInstance().createConnectorInstance(associateId, pdkId, group, version);
            if(nodeInstance == null)
                throw new CoreException(PDKRunnerErrorCodes.PDK_PROCESSOR_NOTFOUND, MessageFormat.format("Source not found for pdkId {0} group {1} version {2} for associateId {3}", pdkId, group, version, associateId));
            SourceNode sourceNode = new SourceNode();
            sourceNode.init((TapConnector) nodeInstance.getTapNode());
            sourceNode.dagId = dagId;
            sourceNode.associateId = associateId;
            sourceNode.tasks = tasks;
            sourceNode.table = table;
            sourceNode.tables = tables;
            sourceNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            sourceNode.connectorContext = new TapConnectorContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), connectionConfig, nodeConfig);
            sourceNode.connectorContext.setTableMap(tableMap);
            sourceNode.connectorContext.setStateMap(stateMap);

            PDKInvocationMonitor.getInstance().invokePDKMethod(sourceNode, PDKMethod.REGISTER_CAPABILITIES,
                    sourceNode::registerCapabilities,
                    MessageFormat.format("call source functions {0} associateId {1}", TapNodeSpecification.idAndGroup(pdkId, group, version), associateId), TAG);
            return sourceNode;
        }
    }

    public static class ConnectorBuilderEx extends ConnectorBuilder<ConnectorNode> {
        public ConnectorNode build() {
            checkParams();
            TapNodeInstance nodeInstance = TapConnectorManager.getInstance().createConnectorInstance(associateId, pdkId, group, version);
            if(nodeInstance == null)
                throw new CoreException(PDKRunnerErrorCodes.PDK_CONNECTOR_NOTFOUND, MessageFormat.format("Source not found for pdkId {0} group {1} version {2} for associateId {3}", pdkId, group, version, associateId));
            ConnectorNode connectorNode = new ConnectorNode();
            connectorNode.init((TapConnector) nodeInstance.getTapNode());
            connectorNode.dagId = dagId;
            connectorNode.associateId = associateId;
            connectorNode.tasks = tasks;
            connectorNode.table = table;
            connectorNode.tables = tables;
            connectorNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            connectorNode.connectorContext = new TapConnectorContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), connectionConfig, nodeConfig);
            connectorNode.connectorContext.setTableMap(tableMap);
            connectorNode.connectorContext.setStateMap(stateMap);

            PDKInvocationMonitor.getInstance().invokePDKMethod(connectorNode, PDKMethod.REGISTER_CAPABILITIES,
                    connectorNode::registerCapabilities,
                    MessageFormat.format("call source functions {0} associateId {1}", TapNodeSpecification.idAndGroup(pdkId, group, version), associateId), TAG);
            return connectorNode;
        }
    }

    public static class TargetConnectorBuilder extends ConnectorBuilder<TargetNode> {
        public TargetNode build() {
            checkParams();
            TapNodeInstance nodeInstance = TapConnectorManager.getInstance().createConnectorInstance(associateId, pdkId, group, version);
            if(nodeInstance == null)
                throw new CoreException(PDKRunnerErrorCodes.PDK_TARGET_NOTFOUND, MessageFormat.format("Target not found for pdkId {0} group {1} version {2} for associateId {3}", pdkId, group, version, associateId));
            TargetNode targetNode = new TargetNode();
            targetNode.dagId = dagId;
            targetNode.associateId = associateId;
            targetNode.tasks = tasks;
            targetNode.table = table;
            targetNode.tables = tables;
            targetNode.init((TapConnector) nodeInstance.getTapNode());
            targetNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            targetNode.connectorContext = new TapConnectorContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), connectionConfig, nodeConfig);
            targetNode.connectorContext.setTableMap(tableMap);
            targetNode.connectorContext.setStateMap(stateMap);

            PDKInvocationMonitor.getInstance().invokePDKMethod(targetNode, PDKMethod.REGISTER_CAPABILITIES,
                    targetNode::registerCapabilities,
                    MessageFormat.format("call target functions {0} associateId {1}", TapNodeSpecification.idAndGroup(pdkId, group, version), associateId), TAG);
            return targetNode;
        }
    }

    public static class ProcessorConnectorBuilder extends ProcessorBuilder<ProcessorNode> {
        public ProcessorNode build() {
            checkParams();
            TapNodeInstance nodeInstance = TapConnectorManager.getInstance().createProcessorInstance(associateId, pdkId, group, version);
            if(nodeInstance == null)
                throw new CoreException(PDKRunnerErrorCodes.PDK_PROCESSOR_NOTFOUND, MessageFormat.format("Processor not found for pdkId {0} group {1} version {2} for associateId {3}", pdkId, group, version, associateId));
            ProcessorNode processorNode = new ProcessorNode();
            processorNode.dagId = dagId;
            processorNode.associateId = associateId;
            processorNode.processor = (TapProcessor) nodeInstance.getTapNode();
            processorNode.processorFunctions = new ProcessorFunctions();
            processorNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            processorNode.processorContext = new TapProcessorContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), connectionConfig);
            PDKInvocationMonitor.getInstance().invokePDKMethod(processorNode, PDKMethod.PROCESSOR_FUNCTIONS,
                    () -> processorNode.processorFunctions(processorNode.processorFunctions),
                    MessageFormat.format("call processor functions {0} associateId {1}", TapNodeSpecification.idAndGroup(pdkId, group, version), associateId), TAG);
            return processorNode;
        }
    }

    public static class SourceAndTargetConnectorBuilder extends ConnectorBuilder<SourceAndTargetNode> {
        public SourceAndTargetNode build() {
            checkParams();
            TapNodeInstance nodeInstance = TapConnectorManager.getInstance().createConnectorInstance(associateId, pdkId, group, version);
            if(nodeInstance == null)
                throw new CoreException(PDKRunnerErrorCodes.PDK_PROCESSOR_NOTFOUND, MessageFormat.format("SourceAndTarget not found for pdkId {0} group {1} version {2} for associateId {3}", pdkId, group, version, associateId));

            TapConnectorContext nodeContext = new TapConnectorContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), connectionConfig, nodeConfig);
            nodeContext.setTableMap(tableMap);
            nodeContext.setStateMap(stateMap);

            ConnectorFunctions connectorFunctions = new ConnectorFunctions();
            TapCodecsRegistry codecRegistry = new TapCodecsRegistry();

            SourceNode sourceNode = new SourceNode();
            sourceNode.init((TapConnector) nodeInstance.getTapNode(), codecRegistry, connectorFunctions);
            sourceNode.dagId = dagId;
            sourceNode.associateId = associateId;
            sourceNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            sourceNode.connectorContext = nodeContext;
            sourceNode.tasks = tasks;
            sourceNode.table = table;
            sourceNode.tables = tables;

            TargetNode targetNode = new TargetNode();
            targetNode.dagId = dagId;
            targetNode.associateId = associateId;
            targetNode.init((TapConnector) nodeInstance.getTapNode(), codecRegistry, connectorFunctions);
            targetNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            targetNode.connectorContext = nodeContext;
            targetNode.tasks = tasks;
            targetNode.table = table;
            targetNode.tables = tables;

            //Source and Target are the same object, will only invoke the method once, no matter source or target, the method is the same.

            PDKInvocationMonitor.getInstance().invokePDKMethod(targetNode, PDKMethod.REGISTER_CAPABILITIES,
                    targetNode::registerCapabilities,
                    MessageFormat.format("call target functions {0} associateId {1}", TapNodeSpecification.idAndGroup(pdkId, group, version), associateId), TAG);
            SourceAndTargetNode sourceAndTargetNode = new SourceAndTargetNode();
            sourceAndTargetNode.dagId = dagId;
            sourceAndTargetNode.associateId = associateId;
            sourceAndTargetNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            sourceAndTargetNode.sourceNode = sourceNode;
            sourceAndTargetNode.targetNode = targetNode;
            return sourceAndTargetNode;
        }
    }

    private static void init() {
        if(tapConnectorManager == null) {
            tapConnectorManager = TapConnectorManager.getInstance().start();
        }
    }

    public static void releaseAssociateId(String associateId) {
        tapConnectorManager.releaseAssociateId(associateId);
    }

    public static void refreshJars() {
        init();
        tapConnectorManager.refreshJars();
    }

    /**
     * use createConnectorBuilder please
     *
     * @return
     */
    @Deprecated
    public static ConnectorBuilder<SourceAndTargetNode> createSourceAndTargetBuilder() {
        init();
        return new SourceAndTargetConnectorBuilder();
    }

    /**
     * use createConnectorBuilder please
     *
     * @return
     */
    @Deprecated
    public static ConnectorBuilder<SourceNode> createSourceBuilder() {
        init();
        return new SourceConnectorBuilder();
    }

    /**
     * use createConnectorBuilder please
     *
     * @return
     */
    @Deprecated
    public static ConnectorBuilder<TargetNode> createTargetBuilder() {
        init();
        return new TargetConnectorBuilder();
    }

    public static ProcessorBuilder<ProcessorNode> createProcessorBuilder() {
        init();
        return new ProcessorConnectorBuilder();
    }

    public static ConnectionConnectorBuilder createConnectionConnectorBuilder() {
        init();
        return new ConnectionConnectorBuilder();
    }

    public static ConnectorBuilder<ConnectorNode> createConnectorBuilder() {
        init();
        return new ConnectorBuilderEx();
    }
}
