package io.tapdata.connector.mongodb.watcher;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.concurrent.atomic.AtomicBoolean;

public class CollectionWatcher implements Runnable {
//    private static final String TAG = CollectionWatcher.class.getSimpleName();
//    private static Logger logger = LoggerFactory.getLogger(CollectionWatcher.class.getSimpleName());
    private MongoCollection<Document> oceanusNodeCollection;
    private final AtomicBoolean isStarted = new AtomicBoolean(true);
    public CollectionWatcher(MongoCollection<Document> collection) {
        oceanusNodeCollection = collection;
    }

    private String resumeToken;

    @Override
    public void run() {

    }

    public void shutdown() {
        if(isStarted.compareAndSet(true, false)) {
            synchronized (isStarted) {
                isStarted.notifyAll();
            }
        }
    }
}
