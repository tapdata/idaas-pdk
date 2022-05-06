package io.tapdata.connector.mysql;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.map.LRUMap;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-05-05 21:18
 **/
public abstract class MysqlWriter {

	private static final String TAG = MysqlWriter.class.getSimpleName();

	abstract public void write(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer);

	protected static class LRUOnRemoveMap<K, V> extends LRUMap<K, V> {

		private Consumer<Entry<K, V>> onRemove;

		public LRUOnRemoveMap(int maxSize, Consumer<Entry<K, V>> onRemove) {
			super(maxSize);
			this.onRemove = onRemove;
		}

		@Override
		protected boolean removeLRU(LinkEntry<K, V> entry) {
			onRemove.accept(entry);
			return super.removeLRU(entry);
		}

		@Override
		public void clear() {
			Set<Entry<K, V>> entries = this.entrySet();
			for (Entry<K, V> entry : entries) {
				onRemove.accept(entry);
			}
			super.clear();
		}

		@Override
		protected void removeEntry(HashEntry<K, V> entry, int hashIndex, HashEntry<K, V> previous) {
			onRemove.accept(entry);
			super.removeEntry(entry, hashIndex, previous);
		}

		@Override
		protected void removeMapping(HashEntry<K, V> entry, int hashIndex, HashEntry<K, V> previous) {
			onRemove.accept(entry);
			super.removeMapping(entry, hashIndex, previous);
		}
	}
}
