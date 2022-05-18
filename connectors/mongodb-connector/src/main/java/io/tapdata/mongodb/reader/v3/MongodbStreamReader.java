package io.tapdata.mongodb.reader.v3;

import io.tapdata.mongodb.bean.MongodbConfig;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.util.List;

/**
 *
 */
public interface MongodbStreamReader {

		void onStart(MongodbConfig mongodbConfig);

		void read(List<String> tableList, Object offset, int eventBatchSize, StreamReadConsumer consumer);

		Object streamOffset(List<String> tableList, Long offsetStartTime);

		void onDestroy();
}
