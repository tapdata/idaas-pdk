package io.tapdata.connector.mysql;

import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

/**
 * @author samuel
 * @Description
 * @create 2022-05-06 20:24
 **/
public interface SqlMaker {
	String[] createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent, String version) throws Throwable;
}
