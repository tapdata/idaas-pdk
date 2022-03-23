package io.tapdata.pdk.core.api;

import io.tapdata.entity.codec.TapCodecRegistry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.TapConnectorNode;
import io.tapdata.entity.utils.DefaultMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.apis.TapProcessor;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.context.TapProcessorContext;
import io.tapdata.pdk.apis.functions.ProcessorFunctions;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.dag.TapDAGNode;
import io.tapdata.pdk.core.error.CoreException;
import io.tapdata.pdk.core.error.ErrorCodes;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.pdk.core.tapnode.TapNodeInstance;

import java.text.MessageFormat;

public class PDKIntegration {
    private static TapConnectorManager tapConnectorManager;

    private static final String TAG = PDKIntegration.class.getSimpleName();

    private PDKIntegration() {}

    public abstract static class ConnectionBuilder<T extends Node> {
        protected String associateId;
        protected DefaultMap connectionConfig;
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

        public ConnectionBuilder<T> withConnectionConfig(DefaultMap connectionConfig) {
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
                throw new CoreException(ErrorCodes.PDK_ILLEGAL_PARAMETER, "Illegal parameter, " + result);
        }

        public abstract T build();
    }

    public abstract static class ProcessorBuilder<T extends Node> {
        protected DefaultMap nodeConfig;
        protected String dagId;

        protected String associateId;
        protected DefaultMap connectionConfig;
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

        public ProcessorBuilder<T> withConnectionConfig(DefaultMap connectionConfig) {
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

        public ProcessorBuilder<T> withNodeConfig(DefaultMap nodeConfig) {
            this.nodeConfig = nodeConfig;
            return this;
        }

        protected void checkParams() {
            String result = verify();
            if(result != null)
                throw new CoreException(ErrorCodes.PDK_ILLEGAL_PARAMETER, "Illegal parameter, " + result);
        }

        public abstract T build();
    }

    public abstract static class ConnectorBuilder<T extends Node> {
        protected TapTable table;
        protected DefaultMap nodeConfig;
        protected String dagId;
        protected String associateId;
        protected DefaultMap connectionConfig;
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
            if(table == null)
                return "missing table";
            if(dagId == null)
                return "missing dagId";
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

        public ConnectorBuilder<T> withVersion(String version) {
            this.version = version;
            return this;
        }

        public ConnectorBuilder<T> withConnectionConfig(DefaultMap connectionConfig) {
            this.connectionConfig = connectionConfig;
            return this;
        }

        public ConnectorBuilder<T> withTapDAGNode(TapDAGNode node) {
            this.pdkId = node.getPdkId();
            this.group = node.getGroup();
            this.version = node.getVersion();
            this.connectionConfig = node.getConnectionConfig();
            this.associateId = node.getId();
            this.table = node.getTable();
            this.nodeConfig = node.getNodeConfig();
            return this;
        }

        public ConnectorBuilder<T> withDagId(String dagId) {
            this.dagId = dagId;
            return this;
        }
        public ConnectorBuilder<T> withTable(TapTable tapTable) {
            this.table = tapTable;
            return this;
        }

        public ConnectorBuilder<T> withNodeConfig(DefaultMap nodeConfig) {
            this.nodeConfig = nodeConfig;
            return this;
        }

        protected void checkParams() {
            String result = verify();
            if(result != null)
                throw new CoreException(ErrorCodes.PDK_ILLEGAL_PARAMETER, "Illegal parameter, " + result);
        }

        public abstract T build();
    }

    public static class ConnectionConnectorBuilder extends ConnectionBuilder<ConnectionNode> {
        public ConnectionNode build() {
            checkParams();
            TapNodeInstance nodeInstance = TapConnectorManager.getInstance().createConnectorInstance(associateId, pdkId, group, version);
            if(nodeInstance == null)
                throw new CoreException(ErrorCodes.PDK_PROCESSOR_NOTFOUND, MessageFormat.format("Source not found for pdkId {0} group {1} version {2} for associateId {3}", pdkId, group, version, associateId));
            ConnectionNode sourceNode = new ConnectionNode();
            sourceNode.connectorNode = (TapConnectorNode) nodeInstance.getTapNode();
            sourceNode.associateId = associateId;
            sourceNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            sourceNode.connectionContext = new TapConnectionContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), connectionConfig);
            return sourceNode;
        }
    }

    public static class SourceConnectorBuilder extends ConnectorBuilder<SourceNode> {
        public SourceNode build() {
            checkParams();
            TapNodeInstance nodeInstance = TapConnectorManager.getInstance().createConnectorInstance(associateId, pdkId, group, version);
            if(nodeInstance == null)
                throw new CoreException(ErrorCodes.PDK_PROCESSOR_NOTFOUND, MessageFormat.format("Source not found for pdkId {0} group {1} version {2} for associateId {3}", pdkId, group, version, associateId));
            SourceNode sourceNode = new SourceNode();
            sourceNode.init((TapConnector) nodeInstance.getTapNode());
            sourceNode.dagId = dagId;
            sourceNode.associateId = associateId;
            sourceNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            sourceNode.connectorContext = new TapConnectorContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), table, connectionConfig, nodeConfig);

            PDKInvocationMonitor.getInstance().invokePDKMethod(PDKMethod.REGISTER_CAPABILITIES,
                    sourceNode::registerCapabilities,
                    MessageFormat.format("call source functions {0} associateId {1}", TapNodeSpecification.idAndGroup(pdkId, group, version), associateId), TAG);
            return sourceNode;
        }
    }

    public static class TargetConnectorBuilder extends ConnectorBuilder<TargetNode> {
        public TargetNode build() {
            checkParams();
            TapNodeInstance nodeInstance = TapConnectorManager.getInstance().createConnectorInstance(associateId, pdkId, group, version);
            if(nodeInstance == null)
                throw new CoreException(ErrorCodes.PDK_TARGET_NOTFOUND, MessageFormat.format("Target not found for pdkId {0} group {1} version {2} for associateId {3}", pdkId, group, version, associateId));
            TargetNode targetNode = new TargetNode();
            targetNode.dagId = dagId;
            targetNode.associateId = associateId;
            targetNode.init((TapConnector) nodeInstance.getTapNode());
            targetNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            targetNode.connectorContext = new TapConnectorContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), table, connectionConfig, nodeConfig);

            PDKInvocationMonitor.getInstance().invokePDKMethod(PDKMethod.REGISTER_CAPABILITIES,
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
                throw new CoreException(ErrorCodes.PDK_PROCESSOR_NOTFOUND, MessageFormat.format("Processor not found for pdkId {0} group {1} version {2} for associateId {3}", pdkId, group, version, associateId));
            ProcessorNode processorNode = new ProcessorNode();
            processorNode.dagId = dagId;
            processorNode.associateId = associateId;
            processorNode.processor = (TapProcessor) nodeInstance.getTapNode();
            processorNode.processorFunctions = new ProcessorFunctions();
            processorNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            processorNode.processorContext = new TapProcessorContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), connectionConfig);
            PDKInvocationMonitor.getInstance().invokePDKMethod(PDKMethod.PROCESSOR_FUNCTIONS,
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
                throw new CoreException(ErrorCodes.PDK_PROCESSOR_NOTFOUND, MessageFormat.format("SourceAndTarget not found for pdkId {0} group {1} version {2} for associateId {3}", pdkId, group, version, associateId));

            TapConnectorContext nodeContext = new TapConnectorContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), table, connectionConfig, nodeConfig);

            ConnectorFunctions connectorFunctions = new ConnectorFunctions();
            TapCodecRegistry codecRegistry = new TapCodecRegistry();

            SourceNode sourceNode = new SourceNode();
            sourceNode.init((TapConnector) nodeInstance.getTapNode(), codecRegistry, connectorFunctions);
            sourceNode.dagId = dagId;
            sourceNode.associateId = associateId;
            sourceNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            sourceNode.connectorContext = nodeContext;

            TargetNode targetNode = new TargetNode();
            targetNode.dagId = dagId;
            targetNode.associateId = associateId;
            targetNode.init((TapConnector) nodeInstance.getTapNode(), codecRegistry, connectorFunctions);
            targetNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            targetNode.connectorContext = nodeContext;

            //Source and Target are the same object, will only invoke the method once, no matter source or target, the method is the same.

            PDKInvocationMonitor.getInstance().invokePDKMethod(PDKMethod.REGISTER_CAPABILITIES,
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

    public static ConnectorBuilder<SourceAndTargetNode> createSourceAndTargetBuilder() {
        init();
        return new SourceAndTargetConnectorBuilder();
    }

    public static ConnectorBuilder<SourceNode> createSourceBuilder() {
        init();
        return new SourceConnectorBuilder();
    }

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

}
