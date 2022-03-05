package ${package};


import io.tapdata.base.ConnectorBase;
import io.tapdata.pdk.apis.TapSource;
import io.tapdata.pdk.apis.TapTarget;
import io.tapdata.pdk.apis.annotations.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionTestResult;
import io.tapdata.pdk.apis.entity.SupportedTapEvents;
import io.tapdata.pdk.apis.entity.TapEvent;
import io.tapdata.pdk.apis.entity.ddl.TapTableOptions;
import io.tapdata.pdk.apis.entity.dml.TapRecordEvent;
import io.tapdata.pdk.apis.functions.SourceFunctions;
import io.tapdata.pdk.apis.functions.TargetFunctions;
import io.tapdata.pdk.apis.functions.consumers.TapListConsumer;
import io.tapdata.pdk.apis.functions.consumers.TapReadOffsetConsumer;
import io.tapdata.pdk.apis.functions.consumers.TapWriteListConsumer;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.util.Arrays;
import java.util.List;

@TapConnector("sourceAndTarget1.json")
public class ${libName}Useless extends ConnectorBase implements TapTarget, TapSource {
  @Override
  public void init(TapConnectorContext connectorContext, TapNodeSpecification specification) {

  }

  @Override
  public ConnectionTestResult connectionTest(TapConnectionContext databaseContext) {
    return null;
  }

  @Override
  public void discoverSchema(TapConnectionContext databaseContext, TapListConsumer<TapTableOptions> tapReadOffsetConsumer) {

  }

  @Override
  public void sourceFunctions(SourceFunctions sourceFunctions) {

  }

  @Override
  public void targetFunctions(TargetFunctions targetFunctions, SupportedTapEvents supportedTapEvents) {

  }

  @Override
  public void destroy() {

  }
}
