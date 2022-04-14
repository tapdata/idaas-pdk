package io.tapdata.pdk.core.monitor;

import io.tapdata.pdk.apis.logger.PDKLogger;
import io.tapdata.pdk.core.error.CoreException;
import io.tapdata.pdk.core.error.ErrorCodes;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * TODO start monitor thread for checking slow invocation
 */
public class PDKInvocationMonitor {
    private static final String TAG = PDKInvocationMonitor.class.getSimpleName();
    private static volatile PDKInvocationMonitor instance;
    private static final Object lock = new int[0];

    private Map<PDKMethod, InvocationCollector> methodInvocationCollectorMap = new ConcurrentHashMap<>();

    private Consumer<String> errorListener;

    private PDKInvocationMonitor() {}

    public void setErrorListener(Consumer<String> errorListener) {
        this.errorListener = errorListener;
    }

    public static PDKInvocationMonitor getInstance() {
        if(instance == null) {
            synchronized (lock) {
                if(instance == null) {
                    instance = new PDKInvocationMonitor();
                }
            }
        }
        return instance;
    }
    public void invokePDKMethod(PDKMethod method, CommonUtils.AnyError r, String message, String logTag) {
        invokePDKMethod(method, r, message, logTag, null, false, 0, 0);
    }
    public void invokePDKMethod(PDKMethod method, CommonUtils.AnyError r, String message, String logTag, Consumer<CoreException> errorConsumer) {
        invokePDKMethod(method, r, message, logTag, errorConsumer, false, 0, 0);
    }
    public void invokePDKMethod(PDKMethod method, CommonUtils.AnyError r, String message, final String logTag, Consumer<CoreException> errorConsumer, boolean async, long retryTimes, long retryPeriodSeconds) {
        if(async) {
            new Thread(() -> {
                if(retryTimes > 0) {
                    CommonUtils.autoRetryAsync(() -> {
                        invokePDKMethodPrivate(method, r, message, logTag, errorConsumer);
                    }, logTag, message, retryTimes, retryPeriodSeconds);
                } else {
                    invokePDKMethodPrivate(method, r, message, logTag, errorConsumer);
                }
            }, "async invoke method " + method.name()).start();
        } else {
            invokePDKMethodPrivate(method, r, message, logTag, errorConsumer);
        }
    }

    private void invokePDKMethodPrivate(PDKMethod method, CommonUtils.AnyError r, String message, String logTag, Consumer<CoreException> errorConsumer) {
        String invokeId = methodStart(method, logTag);
        Throwable theError = null;
        try {
            r.run();
        } catch(CoreException coreException) {
            theError = coreException;

            if(errorConsumer != null) {
                errorConsumer.accept(coreException);
            } else {
                if(errorListener != null)
                    errorListener.accept(describeError(method, coreException, message, logTag));
                throw coreException;
            }
        } catch(Throwable throwable) {
            throwable.printStackTrace();
            theError = throwable;

            CoreException coreException = new CoreException(ErrorCodes.COMMON_UNKNOWN, throwable.getMessage(), throwable);
            if(errorConsumer != null) {
                errorConsumer.accept(coreException);
            } else {
                if(errorListener != null)
                    errorListener.accept(describeError(method, throwable, message, logTag));
                throw coreException;
            }
        } finally {
            methodEnd(method, invokeId, theError, message, logTag);
        }
    }

    private String describeError(PDKMethod method, Throwable throwable, String message, String logTag) {
        return logTag + ": Invoke PDKMethod " + method.name() + " failed, error " + throwable.getMessage() + " context message " + message;
    }

    public String methodStart(PDKMethod method, String logTag) {
        final String invokeId = CommonUtils.processUniqueId();
        InvocationCollector collector = methodInvocationCollectorMap.computeIfAbsent(method, InvocationCollector::new);
        collector.getInvokeIdTimeMap().put(invokeId, System.currentTimeMillis());
        PDKLogger.debug(logTag, "methodStart {} invokeId {}", method, invokeId);
        return invokeId;
    }

    public Long methodEnd(PDKMethod method, String invokeId, Throwable error, String message, String logTag) {
        InvocationCollector collector = methodInvocationCollectorMap.get(method);
        if(collector != null) {
            Long time = collector.getInvokeIdTimeMap().remove(invokeId);
            if(time != null) {
                collector.getCounter().increment();
                long takes = System.currentTimeMillis() - time;
                collector.getTotalTakes().add(takes);
                if(error != null) {
                    PDKLogger.error(logTag, "methodEnd {} invoke {} failed, {} message {} takes {}", method, invokeId, error.getMessage(), message, takes);
                } else {
                    PDKLogger.debug(logTag, "methodEnd {} invoke {} successfully, message {} takes {}", method, invokeId, message, takes);
                }
                return takes;
            }
        }
        return null;
    }

    public static void main(String... args) {
        long time = System.currentTimeMillis();
        for(int i = 0; i < 1000000; i++) {
            UUID.randomUUID().toString();
        }
        System.out.println("takes " + (System.currentTimeMillis() - time));

        AtomicLong counter = new AtomicLong(0);
        time = System.currentTimeMillis();
        String id = null;
        for(int i = 0; i < 1000000; i++) {
            id = Long.toHexString(System.currentTimeMillis()) + Long.toHexString(counter.getAndIncrement());
        }
        System.out.println("takes " + (System.currentTimeMillis() - time) + " id " + id);
    }
}
