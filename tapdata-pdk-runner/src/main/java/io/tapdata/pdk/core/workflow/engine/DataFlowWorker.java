package io.tapdata.pdk.core.workflow.engine;

import io.tapdata.pdk.core.error.CoreException;
import io.tapdata.pdk.core.error.ErrorCodes;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.utils.Validator;
import io.tapdata.pdk.core.utils.state.StateListener;
import io.tapdata.pdk.core.utils.state.StateMachine;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class DataFlowWorker {
    private static final String TAG = DataFlowWorker.class.getSimpleName();

    private TapDAGWithWorker dag;
    private JobOptions jobOptions;
    private static final String STATE_NONE = "None";
    private static final String STATE_INITIALIZING = "Initializing";
    private static final String STATE_INITIALIZED = "Initialized";
    private static final String STATE_INITIALIZE_FAILED = "Initialize failed";
    private static final String STATE_TERMINATED = "Terminated";
    private StateMachine<String, DataFlowWorker> stateMachine;
    private AtomicBoolean started = new AtomicBoolean(false);
    private LastError lastError;
//    private SingleThreadQueue<Runnable> singleThreadQueue;
    private ExecutorService workerThread;

    public static class LastError {
        private CoreException coreException;
        private String fromState;
        private String toState;

        public LastError(CoreException coreException, String fromState, String toState) {
            this.coreException = coreException;
            this.fromState = fromState;
            this.toState = toState;
        }

        public CoreException getCoreException() {
            return coreException;
        }

        public void setCoreException(CoreException coreException) {
            this.coreException = coreException;
        }

        public String getFromState() {
            return fromState;
        }

        public void setFromState(String fromState) {
            this.fromState = fromState;
        }

        public String getToState() {
            return toState;
        }

        public void setToState(String toState) {
            this.toState = toState;
        }
    }

    DataFlowWorker() {
    }

    private void initStateMachine() {
        if(stateMachine == null) {
            stateMachine = new StateMachine<>(DataFlowWorker.class.getSimpleName() + "#" + dag.getId(), STATE_NONE, this);
//        getServiceNodesHandler = StateOperateRetryHandler.build(stateMachine, CoreRuntime.getExecutorsManager().getSystemScheduledExecutorService()).setMaxRetry(5).setRetryInterval(2000L)
//                .setOperateListener(this::handleGetServicesNodes)
//                .setOperateFailedListener(this::handleGetServicesNodesFailed);
            stateMachine.configState(STATE_NONE, stateMachine.execute().nextStates(STATE_INITIALIZING, STATE_TERMINATED))
                    .configState(STATE_INITIALIZING, stateMachine.execute(this::handleInitializing).nextStates(STATE_INITIALIZED, STATE_TERMINATED, STATE_INITIALIZE_FAILED))
                    .configState(STATE_INITIALIZE_FAILED, stateMachine.execute(this::handleInitializeFailed).nextStates(STATE_INITIALIZING, STATE_TERMINATED))
                    .configState(STATE_INITIALIZED, stateMachine.execute(this::handleInitialized).nextStates(STATE_TERMINATED))
                    .configState(STATE_TERMINATED, stateMachine.execute(this::handleTerminated).nextStates(STATE_INITIALIZING, STATE_NONE))
                    .errorOccurred(this::handleError);
            stateMachine.enableAsync(Executors.newSingleThreadExecutor());
        }
    }

    private void handleInitializeFailed(DataFlowWorker dataFlowWorker, StateMachine<String, DataFlowWorker> stringDataFlowWorkerStateMachine) {
    }

    private void handleTerminated(DataFlowWorker dataFlowWorker, StateMachine<String, DataFlowWorker> integerDataFlowWorkerStateMachine) {
    }

    private void handleInitialized(DataFlowWorker dataFlowWorker, StateMachine<String, DataFlowWorker> integerDataFlowWorkerStateMachine) {
        //Start head nodes
        List<String> headNodeIds = dag.getHeadNodeIds();
        for(String nodeId : headNodeIds) {
            TapDAGNodeWithWorker nodeWorker = dag.getNodeMap().get(nodeId);
            if(nodeWorker.sourceNodeDriver != null) {
                nodeWorker.sourceNodeDriver.start();
            }
        }
    }

    private void handleInitializing(DataFlowWorker dataFlowWorker, StateMachine<String, DataFlowWorker> integerDataFlowWorkerStateMachine) {
        List<String> headNodeIds = dag.getHeadNodeIds();
        checkAllNodesInDAG(dag);

        //Setup all nodes, build path for nodes. s
        for(String nodeId : headNodeIds) {
            TapDAGNodeWithWorker nodeWorker = dag.getNodeMap().get(nodeId);
            nodeWorker.setup(dag, jobOptions);
        }

        stateMachine.gotoState(STATE_INITIALIZED, "DataFlow " + dag.getId() + " init successfully");
    }

    private void checkAllNodesInDAG(TapDAGWithWorker dagNodes) {
        //TODO check all node information are correct, PDK can be found correctly.
        //If startNodes not source, or tail nodes not target, remove them.
    }

    public synchronized void start() {
        if(stateMachine.getCurrentState().equals(STATE_NONE)) {
            if(started.compareAndSet(false, true)) {
                //Init every nodes in this data flow.
                stateMachine.gotoState(STATE_INITIALIZING, "Dataflow " + dag.getId() + " start initializing");
            }
        } else {
            throw new CoreException(ErrorCodes.MAIN_DATAFLOW_WORKER_ILLEGAL_STATE, "Dataflow worker state is illegal to init, current state is " + stateMachine.getCurrentState() + " expect state " + STATE_NONE);
        }
    }

    private void handleError(Throwable throwable, String fromState, String toState, DataFlowWorker dataFlowWorker, StateMachine<String, DataFlowWorker> stateMachine) {
        lastError = new LastError(CommonUtils.generateCoreException(throwable), fromState, toState);
    }

    public synchronized void stop() {

    }

    public synchronized void init(TapDAGWithWorker newDag, JobOptions jobOptions) {
        Validator.checkNotNull(ErrorCodes.MAIN_DAG_IS_ILLEGAL, newDag);
        Validator.checkAllNotNull(ErrorCodes.MAIN_DAG_IS_ILLEGAL, newDag.getId(), newDag.getHeadNodeIds());

        if(this.dag == null)
            this.dag = newDag;
        else
            throw new CoreException(ErrorCodes.MAIN_DAG_WORKER_STARTED_ALREADY, "DAG worker has started for data flow " + this.dag.getId() + ", the data flow " + newDag.getId() + " can not start again");

        if(jobOptions == null)
            jobOptions = new JobOptions();

        this.jobOptions = jobOptions;
        initStateMachine();
        //TODO not necessary to use one thread for a worker.
//        workerThread = ExecutorsManager.getInstance().newSingleThreadExecutorService("Dataflow " + dag.getId());
//        stateMachine.enableAsync(workerThread);
    }

    public String getCurrentState() {
        if(stateMachine != null) {
            return stateMachine.getCurrentState();
        }
        return STATE_NONE;
    }

    public void addStateListener(StateListener<String, DataFlowWorker> stateListener) {
        if(stateMachine != null) {
            stateMachine.addStateListener(stateListener);
        }
    }

    public void removeStateListener(StateListener<String, DataFlowWorker> stateListener) {
        if(stateMachine != null) {
            stateMachine.removeStateListener(stateListener);
        }
    }
}
