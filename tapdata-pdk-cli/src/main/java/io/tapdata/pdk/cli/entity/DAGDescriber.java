package io.tapdata.pdk.cli.entity;

import com.alibaba.fastjson.JSON;
import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.core.workflow.engine.JobOptions;
import io.tapdata.pdk.core.workflow.engine.TapDAGNodeWithWorker;
import io.tapdata.pdk.core.workflow.engine.TapDAGWithWorker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class DAGDescriber {
    private static final String TAG = DAGDescriber.class.getSimpleName();

    private String id;
    private List<TapDAGNodeWithWorker> nodes;
    private JobOptions jobOptions;
    private List<List<String>> dag;

    public JobOptions getJobOptions() {
        return jobOptions;
    }

    public void setJobOptions(JobOptions jobOptions) {
        this.jobOptions = jobOptions;
    }

    public List<TapDAGNodeWithWorker> getNodes() {
        return nodes;
    }

    public void setNodes(List<TapDAGNodeWithWorker> nodes) {
        this.nodes = nodes;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<List<String>> getDag() {
        return dag;
    }

    public void setDag(List<List<String>> dag) {
        this.dag = dag;
    }

    public TapDAGWithWorker toDag() {
        if(nodes != null) {
            TapDAGWithWorker dagWithWorker = new TapDAGWithWorker();
            if(id == null) {
                PDKLogger.error(TAG, "Missing dag id while generating DAG");
                return null;
            }
            dagWithWorker.setId(id);
            List<String> headNodeIds = new CopyOnWriteArrayList<>();
            dagWithWorker.setHeadNodeIds(headNodeIds);
            Map<String, TapDAGNodeWithWorker> nodeMap = new ConcurrentHashMap<>();
            dagWithWorker.setNodeMap(nodeMap);
            //Put into a map
            for(TapDAGNodeWithWorker node : nodes) {
                if(node == null) continue;
                String result = node.verify();
                if(result != null) {
                    PDKLogger.warn(TAG, "Node verify failed, {} node json {}", result, JSON.toJSONString(node));
                    continue;
                }
                TapDAGNodeWithWorker old = nodeMap.put(node.getId(), node);
                if(old != null) {
                    PDKLogger.warn(TAG, "Node id {} is duplicated, node is replaced, removed node json {}", node.getId(), JSON.toJSONString(old));
                }
            }

            //Repair child and parent
            if(this.dag != null) {
                for(List<String> dagLine : this.dag) {
                    if(dagLine.size() != 2) {
                        PDKLogger.warn(TAG, "DagLine is illegal {}", Arrays.toString(dagLine.toArray()));
                        continue;
                    }
                    TapDAGNodeWithWorker startNode = nodeMap.get(dagLine.get(0));
                    TapDAGNodeWithWorker endNode = nodeMap.get(dagLine.get(1));
                    List<String> childNodeIds = startNode.getChildNodeIds();
                    if(childNodeIds == null) {
                        childNodeIds = new ArrayList<>();
                        startNode.setChildNodeIds(childNodeIds);
                    }
                    if(!childNodeIds.contains(endNode.getId())) {
                        childNodeIds.add(endNode.getId());
                    }

                    List<String> parentNodeIds = endNode.getParentNodeIds();
                    if(parentNodeIds == null) {
                        parentNodeIds = new ArrayList<>();
                        endNode.setParentNodeIds(parentNodeIds);
                    }
                    if(!parentNodeIds.contains(startNode.getId())) {
                        parentNodeIds.add(startNode.getId());
                    }
                }
            }

            //Prepare head node id list
            for(TapDAGNodeWithWorker node : nodes) {
                if(node.getParentNodeIds() == null || node.getParentNodeIds().isEmpty()) {
                    if(node.getChildNodeIds() != null && !node.getChildNodeIds().isEmpty()) {
                        if(!headNodeIds.contains(node.getId()))
                            headNodeIds.add(node.getId());
                    } else {
                        PDKLogger.warn(TAG, "Node in DAG don't have parent node ids or child node ids, the node {} will be ignored", node.getId());
                        nodeMap.remove(node.getId());
                    }
                }
            }
            if(dagWithWorker.getNodeMap().isEmpty() || dagWithWorker.getHeadNodeIds().isEmpty()) {
                PDKLogger.error(TAG, "Generated dag don't have node map or head node ids, dag json {}", JSON.toJSONString(dagWithWorker));
            } else {
                PDKLogger.info(TAG, "DAG tree: {}", dagWithWorker.dagString());
                return dagWithWorker;
            }
        }
        return null;
    }
}
