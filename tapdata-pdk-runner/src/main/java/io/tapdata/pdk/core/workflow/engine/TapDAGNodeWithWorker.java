package io.tapdata.pdk.core.workflow.engine;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.api.SourceAndTargetNode;
import io.tapdata.pdk.core.dag.TapDAGNode;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.queue.ListHandler;
import io.tapdata.pdk.core.utils.queue.SingleThreadBlockingQueue;
import io.tapdata.pdk.core.workflow.engine.driver.Driver;
import io.tapdata.pdk.core.workflow.engine.driver.ProcessorNodeDriver;
import io.tapdata.pdk.core.workflow.engine.driver.SourceNodeDriver;
import io.tapdata.pdk.core.workflow.engine.driver.TargetNodeDriver;

import java.util.List;

public class TapDAGNodeWithWorker extends TapDAGNode {
    ProcessorNodeDriver processorNodeDriver;
    SourceNodeDriver sourceNodeDriver;
    TargetNodeDriver targetNodeDriver;

    public void setup(TapDAGWithWorker dag, JobOptions jobOptions) {
        switch (type) {
            case TapDAGNode.TYPE_SOURCE:
                if(sourceNodeDriver == null) {
                    sourceNodeDriver = new SourceNodeDriver();
                }
                if(sourceNodeDriver.getSourceNode() == null) {
                    sourceNodeDriver.setSourceNode(PDKIntegration.createSourceBuilder()
                            .withTapDAGNode(this)
                            .withDagId(dag.getId())
                            .build());
                }
                break;
            case TapDAGNode.TYPE_PROCESSOR:
                if(processorNodeDriver == null) {
                    processorNodeDriver = new ProcessorNodeDriver();
                }
                if(processorNodeDriver.getProcessorNode() == null) {
                    processorNodeDriver.setProcessorNode(PDKIntegration.createProcessorBuilder()
                            .withTapDAGNode(this)
                            .withDagId(dag.getId())
                            .build());
                }
                break;
            case TapDAGNode.TYPE_SOURCE_TARGET:
                SourceAndTargetNode sourceAndTargetNode = PDKIntegration.createSourceAndTargetBuilder()
                        .withTapDAGNode(this)
                        .withDagId(dag.getId())
                        .build();
                if(sourceNodeDriver == null) {
                    sourceNodeDriver = new SourceNodeDriver();
                }
                if(sourceNodeDriver.getSourceNode() == null) {
                    sourceNodeDriver.setSourceNode(sourceAndTargetNode.getSourceNode());
                }

                if(targetNodeDriver == null) {
                    targetNodeDriver = new TargetNodeDriver();
                }
                if(targetNodeDriver.getTargetNode() == null) {
                    targetNodeDriver.setTargetNode(sourceAndTargetNode.getTargetNode());
                }
                break;
            case TapDAGNode.TYPE_TARGET:
                if(targetNodeDriver == null) {
                    targetNodeDriver = new TargetNodeDriver();
                }
                if(targetNodeDriver.getTargetNode() == null) {
                    targetNodeDriver.setTargetNode(PDKIntegration.createTargetBuilder()
                            .withTapDAGNode(this)
                            .withDagId(dag.getId())
                            .build());
                }
                break;
        }
        if(childNodeIds != null) {
            for(String childNodeId : childNodeIds) {
                TapDAGNodeWithWorker nodeWorker = dag.getNodeMap().get(childNodeId);
                if(nodeWorker != null) {
                    nodeWorker.setup(dag, jobOptions);
                    buildPath(this, nodeWorker, jobOptions);
                }
            }
        }
    }

    private void buildPath(TapDAGNodeWithWorker parent, TapDAGNodeWithWorker child, JobOptions jobOptions) {
        String queueName = parent.id + " pdk " + TapNodeSpecification.idAndGroup(parent.pdkId, parent.group, parent.version);
        if(parent.sourceNodeDriver != null) {
            connect(parent.sourceNodeDriver, child.processorNodeDriver, jobOptions, "Source queue " + queueName + " to processor " + child.id);
            connect(parent.sourceNodeDriver, child.targetNodeDriver, jobOptions, "Source queue " + queueName + " to target " + child.id);
        }
        if(parent.processorNodeDriver != null) {
            connect(parent.processorNodeDriver, child.processorNodeDriver, jobOptions, "Processor queue " + queueName + " to processor " + child.id);
            connect(parent.processorNodeDriver, child.targetNodeDriver, jobOptions, "Processor queue " + queueName + " to target " + child.id);
        }
    }

    private void connect(Driver driver, ListHandler<List<TapEvent>> queueReceiver, JobOptions jobOptions, String queueName) {
        if(driver != null && queueReceiver != null) {
            SingleThreadBlockingQueue<List<TapEvent>> queue = new SingleThreadBlockingQueue<List<TapEvent>>(queueName)
                    .withMaxSize(jobOptions.getQueueSize())
                    .withHandleSize(jobOptions.getQueueBatchSize())
                    .withExecutorService(ExecutorsManager.getInstance().getExecutorService())
                    .withHandler(queueReceiver)
                    .start();
            driver.registerQueue(queue);
        }
    }
}
