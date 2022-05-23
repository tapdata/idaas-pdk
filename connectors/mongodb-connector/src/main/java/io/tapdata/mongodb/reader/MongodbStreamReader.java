package io.tapdata.mongodb.reader;

import io.tapdata.mongodb.bean.MongodbConfig;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.List;
import java.util.function.BiConsumer;

/**
 *
 */
public interface MongodbStreamReader {

		void onStart(MongodbConfig mongodbConfig);

		void read(List<String> tableList, Object offset, int eventBatchSize, StreamReadConsumer consumer) throws Exception;


		Object streamOffset(Long offsetStartTime);
		void onDestroy();
}
