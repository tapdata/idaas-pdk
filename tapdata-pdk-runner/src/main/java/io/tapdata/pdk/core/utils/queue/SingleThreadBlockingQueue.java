package io.tapdata.pdk.core.utils.queue;

import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.core.error.CoreException;
import io.tapdata.pdk.core.error.ErrorCodes;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.core.workflow.engine.DataFlowEngine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 *
 * @param <T>
 */
public class SingleThreadBlockingQueue<T> implements Runnable {
    private static final String TAG = SingleThreadBlockingQueue.class.getSimpleName();
    private ExecutorService threadPoolExecutor;
    private int maxSize = 20;
    private final Object lock = new Object();
    private volatile AtomicBoolean isFull = new AtomicBoolean(false);
    private LinkedBlockingQueue<T> queue;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicBoolean isStopping = new AtomicBoolean(false);
    private ListHandler<T> listHandler;
    private ListErrorHandler<T> listErrorHandler;
    private int handleSize = 20;
//    private List<T> handleList = new ArrayList<>();
    protected String name;
    private LongAdder counter = new LongAdder();
    private int notifySize;
    private LongAdder notifyCounter = new LongAdder();


//    private SingleThreadQueue<T> ensureSingleThreadInputQueue;

    public SingleThreadBlockingQueue(String name){
        this.name = name;
    }
    public SingleThreadBlockingQueue<T> withExecutorService(ExecutorService executorService) {
        this.threadPoolExecutor = executorService;
        return this;
    }

    /**
     * The batch size when consume data.
     *
     * @param size
     * @return
     */
    public SingleThreadBlockingQueue<T> withHandleSize(int size) {
        handleSize = size;
        return this;
    }

    /**
     * The batch handler for consuming data.
     *
     * @param listHandler
     * @return
     */
    public SingleThreadBlockingQueue<T> withHandler(ListHandler<T> listHandler) {
        this.listHandler = listHandler;
        return this;
    }

    /**
     * The batch handler when error occurred.
     *
     * @param listErrorHandler
     * @return
     */
    public SingleThreadBlockingQueue<T> withErrorHandler(ListErrorHandler<T> listErrorHandler) {
        this.listErrorHandler = listErrorHandler;
        return this;
    }

    /**
     * Queue max size.
     * When reach the max size, the queue will block enqueue thread.
     *
     * @param maxSize
     * @return
     */
    public SingleThreadBlockingQueue<T> withMaxSize(int maxSize) {
        this.maxSize = maxSize;
        return this;
    }
    private void startPrivate(){
        if(isStopping.get())
            throw new CoreException(ErrorCodes.COMMON_SINGLE_THREAD_QUEUE_STOPPED, "SingleThreadBlockingQueue is stopped");
        if(isRunning.compareAndSet(false, true)){
            threadPoolExecutor.execute(this);
        }
    }
    public SingleThreadBlockingQueue<T> start(){
        if(isStopping.get())
            throw new CoreException(ErrorCodes.COMMON_SINGLE_THREAD_QUEUE_STOPPED, "SingleThreadBlockingQueue is stopped");
        if(threadPoolExecutor == null)
            throw new CoreException(ErrorCodes.COMMON_SINGLE_THREAD_BLOCKING_QUEUE_NO_EXECUTOR, "SingleThreadBlockingQueue " + name + " no threadPoolExecutor");

        if(queue == null) {
            queue = new LinkedBlockingQueue<>(maxSize);
            notifySize = maxSize / 2;
        }
        startPrivate();
        return this;
    }
    @Override
    public void run() {
        boolean end = false;
        while (!end && !isStopping.get()) {
            if(queue.isEmpty()) {
                synchronized (lock) {
                    if(queue.isEmpty()) {
                        isRunning.compareAndSet(true, false);
                        end = true;
                    }
                }
            } else {
                try {
                    List<T> handleList = new ArrayList<>();
                    synchronized (this) {
                        T t = queue.poll();
                        while(t != null) {
                            handleList.add(t);
                            consumed(t);
                            if(handleList.size() >= handleSize)
                                break;
                            t = queue.poll();
                            if(t == null)
                                break;
                        }
                    }
                    if(!isStopping.get() && !handleList.isEmpty()) {
                        execute(handleList);
                    }
//                    handleList.clear();
                }  catch(Throwable throwable) {
                    throwable.printStackTrace();
                    PDKLogger.error(TAG, "{} occurred unknown error, {}", name, throwable.getMessage());
                }
            }
        }
    }

    private void execute(List<T> t) {
        counter.add(t.size());
        try {
            this.listHandler.execute(t);
        } catch (Throwable e) {
            if(listErrorHandler != null) {
                CommonUtils.ignoreAnyError(() -> {
                    this.listErrorHandler.error(t, e);
                }, TAG);
            }
        }
    }


    private synchronized void input(T t) {
        try {
            if(queue.isEmpty()){
                synchronized (lock){
                    queue.add(t);
                }
            } else {
                queue.add(t);
            }
        } catch(IllegalStateException e) {
            if(e.getMessage().contains("full")) {
                isFull.set(true);
//                logger.debug("{} queue is full, wait polling to add more {}", name, queue.size());
                while(isFull.get()) {
                    try {
                        this.wait(120000);
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                        PDKLogger.error(TAG, "{} is interrupted, {}", name, interruptedException.getMessage());
                        Thread.currentThread().interrupt();
                    }
                }
//                    logger.debug("wake up to add {}", t);
                offer(t);
//                    logger.debug("wake up to added {}", t);
            }
        }
    }

    public void add(T t) {
        offer(t);
    }

    public void offer(T t) {
        if(queue == null)
            throw new CoreException(ErrorCodes.COMMON_ILLEGAL_PARAMETERS, "Queue is not initialized");
        if(isStopping.get())
            throw new CoreException(ErrorCodes.COMMON_SINGLE_THREAD_QUEUE_STOPPED, "SingleThreadQueue is stopped");

//        ensureSingleThreadInputQueue.offerAndStart(t);
        input(t);
        startPrivate();
    }

    public void stop() {
        if(isStopping.compareAndSet(false, true)) {
            clear();
        }
    }

    public void clear() {
        queue.clear();
    }

    public ListHandler<T> getHandler() {
        return listHandler;
    }

    public Queue<T> getQueue() {
        return queue;
    }

    public String getName() {
        return name;
    }

    protected void consumed(T t) {
//        logger.info("queue size {}", getQueue().size());
        if(isFull.get()) {
            notifyCounter.increment();
            if(notifyCounter.longValue() > notifySize || queue.isEmpty()) {
//                logger.info("123 queue size {} notifyCounter {} notifySize {}", getQueue().size(), notifyCounter.longValue(), notifySize);
                if(isFull.compareAndSet(true, false)) {
                    this.notifyAll();
                    notifyCounter.reset();
                }
//                logger.info("notifyAll queue size {} notifyCounter {} notifySize {}", getQueue().size(), notifyCounter.longValue(), notifySize);
//                synchronized (lock) {
//                    if(isFull && (notifyCounter.longValue() > notifySize || queue.isEmpty())) {
//
//                    }
//                }

            }
        }
    }

    public static void main(String... args) {
//        LinkedBlockingQueue<String> queue1 = new LinkedBlockingQueue<>(4);
//        for(int i = 0; i < 10; i++) {
//            queue1.add("aaa " + i);
//            logger.info("aaa " + i);
//        }
//        String v = null;
//        while((v = queue1.poll()) != null) {
//            logger.info(v);
//        }
//        if(true)
//            return;
        DataFlowEngine.getInstance().start();
        ExecutorService executorService = Executors.newFixedThreadPool(10);
//        Logger logger = LoggerFactory.getLogger("aaa");
        SingleThreadBlockingQueue<String> queue = new SingleThreadBlockingQueue<String>("aaa")
                .withMaxSize(20)
                .withHandleSize(5)
                .withExecutorService(ExecutorsManager.getInstance().getExecutorService())
                .withHandler(o -> {
//                    Thread.sleep(10);
                    PDKLogger.info(TAG, Arrays.toString(o.toArray()));
                })
                .withErrorHandler((o, throwable) -> {
                    PDKLogger.error(TAG, Arrays.toString(o.toArray()));
                }).start();
                long time = System.currentTimeMillis();
        for(int i = 0; i < 4000; i++) {
            final int value = i;
            executorService.submit(() -> {
                queue.offer("hello " + value);
//                try {
//                    Thread.sleep(10);
//                } catch (InterruptedException interruptedException) {
//                    interruptedException.printStackTrace();
//                }
            });
        }
    }

    public long counter() {
        return counter.longValue();
    }
}
