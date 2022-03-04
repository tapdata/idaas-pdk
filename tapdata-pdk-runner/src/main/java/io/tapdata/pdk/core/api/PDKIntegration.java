package io.tapdata.pdk.core.api;

import io.tapdata.pdk.apis.TapConnectorNode;
import io.tapdata.pdk.apis.common.DefaultMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.SupportedTapEvents;
import io.tapdata.pdk.apis.entity.ddl.TapTableOptions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.apis.TapRecordProcessor;
import io.tapdata.pdk.apis.TapSource;
import io.tapdata.pdk.apis.TapTarget;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.context.TapProcessorContext;
import io.tapdata.pdk.apis.functions.ProcessorFunctions;
import io.tapdata.pdk.apis.functions.SourceFunctions;
import io.tapdata.pdk.apis.functions.TargetFunctions;
import io.tapdata.pdk.apis.entity.ddl.TapTable;
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

    public abstract static class DatabaseBuilder<T extends Node> {
        protected String associateId;
        protected DefaultMap connectionConfig;
        protected String pdkId;
        protected String group;
        protected Integer minBuildNumber;

        public String verify() {
            if(associateId == null)
                return "missing associateId";
            if(pdkId == null)
                return "missing pdkId";
            if(group == null)
                return "missing group";
            if(minBuildNumber == null)
                return "missing minBuildNumber";
            return null;
        }

        /**
         * Each
         * @param associateId
         * @return
         */
        public DatabaseBuilder<T> withAssociateId(String associateId) {
            this.associateId = associateId;
            return this;
        }

        public DatabaseBuilder<T> withPdkId(String pdkId) {
            this.pdkId = pdkId;
            return this;
        }

        public DatabaseBuilder<T> withGroup(String group) {
            this.group = group;
            return this;
        }

        public DatabaseBuilder<T> withMinBuildNumber(Integer minBuildNumber) {
            this.minBuildNumber = minBuildNumber;
            return this;
        }

        public DatabaseBuilder<T> withConnectionConfig(DefaultMap connectionConfig) {
            this.connectionConfig = connectionConfig;
            return this;
        }

        public DatabaseBuilder<T> withTapDAGNode(TapDAGNode node) {
            this.pdkId = node.getPdkId();
            this.group = node.getGroup();
            this.minBuildNumber = node.getMinBuildNumber();
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
        protected Integer minBuildNumber;

        public String verify() {
            if(associateId == null)
                return "missing associateId";
            if(pdkId == null)
                return "missing pdkId";
            if(group == null)
                return "missing group";
            if(minBuildNumber == null)
                return "missing minBuildNumber";
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

        public ProcessorBuilder<T> withMinBuildNumber(Integer minBuildNumber) {
            this.minBuildNumber = minBuildNumber;
            return this;
        }

        public ProcessorBuilder<T> withConnectionConfig(DefaultMap connectionConfig) {
            this.connectionConfig = connectionConfig;
            return this;
        }

        public ProcessorBuilder<T> withTapDAGNode(TapDAGNode node) {
            this.pdkId = node.getPdkId();
            this.group = node.getGroup();
            this.minBuildNumber = node.getMinBuildNumber();
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
        protected Integer minBuildNumber;

        public String verify() {
            if(associateId == null)
                return "missing associateId";
            if(pdkId == null)
                return "missing pdkId";
            if(group == null)
                return "missing group";
            if(minBuildNumber == null)
                return "missing minBuildNumber";
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

        public ConnectorBuilder<T> withMinBuildNumber(Integer minBuildNumber) {
            this.minBuildNumber = minBuildNumber;
            return this;
        }

        public ConnectorBuilder<T> withConnectionConfig(DefaultMap connectionConfig) {
            this.connectionConfig = connectionConfig;
            return this;
        }

        public ConnectorBuilder<T> withTapDAGNode(TapDAGNode node) {
            this.pdkId = node.getPdkId();
            this.group = node.getGroup();
            this.minBuildNumber = node.getMinBuildNumber();
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

    public static class DatabaseConnectorBuilder extends DatabaseBuilder<DatabaseNode> {
        public DatabaseNode build() {
            checkParams();
            TapNodeInstance nodeInstance = TapConnectorManager.getInstance().createConnectorInstance(associateId, pdkId, group, minBuildNumber);
            if(nodeInstance == null)
                throw new CoreException(ErrorCodes.PDK_PROCESSOR_NOTFOUND, MessageFormat.format("Source not found for pdkId {0} group {1} minBuildNumber {2} for associateId {3}", pdkId, group, minBuildNumber, associateId));
            DatabaseNode sourceNode = new DatabaseNode();
            sourceNode.connectorNode = (TapConnectorNode) nodeInstance.getTapNode();
            sourceNode.associateId = associateId;
            sourceNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            sourceNode.databaseContext = new TapConnectionContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), connectionConfig);
//            sourceNode.databaseContext.setLogger(LogManager.getLogger(nodeInstance.getTapNodeInfo().getNodeClass()));
            return sourceNode;
        }
    }

    public static class SourceConnectorBuilder extends ConnectorBuilder<SourceNode> {
        public SourceNode build() {
            checkParams();
            TapNodeInstance nodeInstance = TapConnectorManager.getInstance().createConnectorInstance(associateId, pdkId, group, minBuildNumber);
            if(nodeInstance == null)
                throw new CoreException(ErrorCodes.PDK_PROCESSOR_NOTFOUND, MessageFormat.format("Source not found for pdkId {0} group {1} minBuildNumber {2} for associateId {3}", pdkId, group, minBuildNumber, associateId));
            SourceNode sourceNode = new SourceNode();
            sourceNode.source = (TapSource) nodeInstance.getTapNode();
            sourceNode.dagId = dagId;
            sourceNode.associateId = associateId;
            sourceNode.sourceFunctions = new SourceFunctions();
            sourceNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            sourceNode.connectorContext = new TapConnectorContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), table, connectionConfig, nodeConfig);
//            sourceNode.connectorContext.setLogger(LogManager.getLogger(nodeInstance.getTapNodeInfo().getNodeClass()));

            TapConnectionContext connectionContext = new TapConnectionContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), sourceNode.getConnectorContext().getConnectionConfig());
//            connectionContext.setLogger(sourceNode.getConnectorContext().getLogger());
            sourceNode.getSource().discoverSchema(connectionContext, (events, error) -> {
                if(events != null) {
                    for(TapTableOptions tableOptions : events) {
                        TapTable table = tableOptions.getTable();
                        if(table != null) {
                            TapTable targetTable = sourceNode.getConnectorContext().getTable();
                            if(targetTable != null && targetTable.getName() != null && targetTable.getName().equals(table.getName())) {
                                sourceNode.getConnectorContext().setTable(table);
                                break;
                            }
                        }
                    }
                }
            });

            PDKInvocationMonitor.getInstance().invokePDKMethod(PDKMethod.SOURCE_INIT,
                    () -> sourceNode.init(sourceNode.tapNodeInfo.getTapNodeSpecification()),
                    MessageFormat.format("create source {0} associate with id {1}", TapNodeSpecification.idAndGroup(pdkId, group), associateId), TAG);
            PDKInvocationMonitor.getInstance().invokePDKMethod(PDKMethod.SOURCE_FUNCTIONS,
                    () -> sourceNode.sourceFunctions(sourceNode.sourceFunctions),
                    MessageFormat.format("call source functions {0} associateId {1}", TapNodeSpecification.idAndGroup(pdkId, group), associateId), TAG);
            return sourceNode;
        }
    }

    public static class TargetConnectorBuilder extends ConnectorBuilder<TargetNode> {
        public TargetNode build() {
            checkParams();
            TapNodeInstance nodeInstance = TapConnectorManager.getInstance().createConnectorInstance(associateId, pdkId, group, minBuildNumber);
            if(nodeInstance == null)
                throw new CoreException(ErrorCodes.PDK_TARGET_NOTFOUND, MessageFormat.format("Target not found for pdkId {0} group {1} minBuildNumber {2} for associateId {3}", pdkId, group, minBuildNumber, associateId));
            TargetNode targetNode = new TargetNode();
            targetNode.dagId = dagId;
            targetNode.associateId = associateId;
            targetNode.target = (TapTarget) nodeInstance.getTapNode();
            targetNode.targetFunctions = new TargetFunctions();
            targetNode.supportedTapEvents = new SupportedTapEvents();
            targetNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            targetNode.connectorContext = new TapConnectorContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), table, connectionConfig, nodeConfig);
//            targetNode.connectorContext.setLogger(LoggerFactory.getLogger(nodeInstance.getTapNodeInfo().getNodeClass()));

            TapConnectionContext connectionContext = new TapConnectionContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), targetNode.getConnectorContext().getConnectionConfig());
//            connectionContext.setLogger(targetNode.getConnectorContext().getLogger());
            targetNode.getTarget().discoverSchema(connectionContext, (events, error) -> {
                if(events != null) {
                    for(TapTableOptions tableOptions : events) {
                        TapTable table = tableOptions.getTable();
                        if(table != null) {
                            TapTable targetTable = targetNode.getConnectorContext().getTable();
                            if(targetTable != null && targetTable.getName() != null && targetTable.getName().equals(table.getName())) {
                                targetNode.getConnectorContext().setTable(table);
                                break;
                            }
                        }
                    }
                }
            });

            PDKInvocationMonitor.getInstance().invokePDKMethod(PDKMethod.TARGET_INIT,
                    () -> targetNode.init(targetNode.tapNodeInfo.getTapNodeSpecification()),
                    MessageFormat.format("create target {0} associate with id {1}", TapNodeSpecification.idAndGroup(pdkId, group), associateId), TAG);
            PDKInvocationMonitor.getInstance().invokePDKMethod(PDKMethod.TARGET_FUNCTIONS,
                    () -> targetNode.targetFunctions(targetNode.targetFunctions),
                    MessageFormat.format("call target functions {0} associateId {1}", TapNodeSpecification.idAndGroup(pdkId, group), associateId), TAG);
            return targetNode;
        }
    }

    public static class ProcessorConnectorBuilder extends ProcessorBuilder<ProcessorNode> {
        public ProcessorNode build() {
            checkParams();
            TapNodeInstance nodeInstance = TapConnectorManager.getInstance().createProcessorInstance(associateId, pdkId, group, minBuildNumber);
            if(nodeInstance == null)
                throw new CoreException(ErrorCodes.PDK_PROCESSOR_NOTFOUND, MessageFormat.format("Processor not found for pdkId {0} group {1} minBuildNumber {2} for associateId {3}", pdkId, group, minBuildNumber, associateId));
            ProcessorNode processorNode = new ProcessorNode();
            processorNode.dagId = dagId;
            processorNode.associateId = associateId;
            processorNode.processor = (TapRecordProcessor) nodeInstance.getTapNode();
            processorNode.processorFunctions = new ProcessorFunctions();
            processorNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            processorNode.processorContext = new TapProcessorContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), connectionConfig);
//            processorNode.processorContext.setLogger(LoggerFactory.getLogger(nodeInstance.getTapNodeInfo().getNodeClass()));
            PDKInvocationMonitor.getInstance().invokePDKMethod(PDKMethod.PROCESSOR_INIT,
                    () -> processorNode.init(processorNode.tapNodeInfo.getTapNodeSpecification()),
                    MessageFormat.format("create processor {0} associate with id {1}", TapNodeSpecification.idAndGroup(pdkId, group), associateId), TAG);
            PDKInvocationMonitor.getInstance().invokePDKMethod(PDKMethod.PROCESSOR_FUNCTIONS,
                    () -> processorNode.processorFunctions(processorNode.processorFunctions),
                    MessageFormat.format("call processor functions {0} associateId {1}", TapNodeSpecification.idAndGroup(pdkId, group), associateId), TAG);
            return processorNode;
        }
    }

    public static class SourceAndTargetConnectorBuilder extends ConnectorBuilder<SourceAndTargetNode> {
        public SourceAndTargetNode build() {
            checkParams();
            TapNodeInstance nodeInstance = TapConnectorManager.getInstance().createConnectorInstance(associateId, pdkId, group, minBuildNumber);
            if(nodeInstance == null)
                throw new CoreException(ErrorCodes.PDK_PROCESSOR_NOTFOUND, MessageFormat.format("SourceAndTarget not found for pdkId {0} group {1} minBuildNumber {2} for associateId {3}", pdkId, group, minBuildNumber, associateId));

            TapConnectorContext nodeContext = new TapConnectorContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), table, connectionConfig, nodeConfig);

            SourceNode sourceNode = new SourceNode();
            sourceNode.source = (TapSource) nodeInstance.getTapNode();
            sourceNode.dagId = dagId;
            sourceNode.associateId = associateId;
            sourceNode.sourceFunctions = new SourceFunctions();
            sourceNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            sourceNode.connectorContext = nodeContext;
//            sourceNode.connectorContext.setLogger(LoggerFactory.getLogger(nodeInstance.getTapNodeInfo().getNodeClass()));

            TargetNode targetNode = new TargetNode();
            targetNode.dagId = dagId;
            targetNode.associateId = associateId;
            targetNode.target = (TapTarget) nodeInstance.getTapNode();
            targetNode.targetFunctions = new TargetFunctions();
            targetNode.supportedTapEvents = new SupportedTapEvents();
            targetNode.tapNodeInfo = nodeInstance.getTapNodeInfo();
            targetNode.connectorContext = nodeContext;
//            targetNode.connectorContext.setLogger(LoggerFactory.getLogger(nodeInstance.getTapNodeInfo().getNodeClass()));

            TapConnectionContext connectionContext = new TapConnectionContext(nodeInstance.getTapNodeInfo().getTapNodeSpecification(), sourceNode.getConnectorContext().getConnectionConfig());
//            connectionContext.setLogger(sourceNode.getConnectorContext().getLogger());
            sourceNode.getSource().discoverSchema(connectionContext, (events, error) -> {
                if(events != null) {
                    for(TapTableOptions tableOptions : events) {
                        TapTable table = tableOptions.getTable();
                        if(table != null) {
                            TapTable targetTable = sourceNode.getConnectorContext().getTable();
                            if(targetTable != null && targetTable.getName() != null && targetTable.getName().equals(table.getName())) {
                                sourceNode.getConnectorContext().setTable(table);
                                targetNode.getConnectorContext().setTable(table);
                                break;
                            }
                        }
                    }
                }
            });

            //Source and Target are the same object, will only invoke the method once, no matter source or target, the method is the same.
            PDKInvocationMonitor.getInstance().invokePDKMethod(PDKMethod.SOURCE_TARGET_INIT,
                    () -> sourceNode.init(sourceNode.tapNodeInfo.getTapNodeSpecification()),
                    MessageFormat.format("create target {0} associate with id {1}", TapNodeSpecification.idAndGroup(pdkId, group), associateId), TAG);
            PDKInvocationMonitor.getInstance().invokePDKMethod(PDKMethod.SOURCE_FUNCTIONS,
                    () -> sourceNode.sourceFunctions(sourceNode.sourceFunctions),
                    MessageFormat.format("call source functions {0} associateId {1}", TapNodeSpecification.idAndGroup(pdkId, group), associateId), TAG);
            PDKInvocationMonitor.getInstance().invokePDKMethod(PDKMethod.TARGET_FUNCTIONS,
                    () -> targetNode.targetFunctions(targetNode.targetFunctions),
                    MessageFormat.format("call target functions {0} associateId {1}", TapNodeSpecification.idAndGroup(pdkId, group), associateId), TAG);
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

    public static DatabaseConnectorBuilder createDatabaseConnectorBuilder() {
        init();
        return new DatabaseConnectorBuilder();
    }

}
