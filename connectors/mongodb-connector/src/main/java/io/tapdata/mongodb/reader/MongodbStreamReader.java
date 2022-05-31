package io.tapdata.mongodb.reader;

import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.util.List;

/**
 *
 */
public interface MongodbStreamReader {

		void onStart(MongodbConfig mongodbConfig);

		void read(List<String> tableList, Object offset, int eventBatchSize, StreamReadConsumer consumer) throws Exception;


		Object streamOffset(Long offsetStartTime);
		void onDestroy();
}
