package ${package};

import io.tapdata.pdk.apis.TapSource;
import io.tapdata.pdk.apis.TapTarget;
import io.tapdata.pdk.apis.annotations.TapConnector;
import io.tapdata.pdk.apis.base.ConnectorBase;
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


@TapConnector("sourceAndTarget.json")
public class ${libName}SourceAndTarget extends ConnectorBase implements TapTarget, TapSource {
  @Override
  public void init(TapConnectorContext connectorContext, TapNodeSpecification specification) {

  }

  @Override
  public ConnectionTestResult connectionTest(TapConnectionContext databaseContext) {
    return null;
  }

  @Override
  public void allTables(TapConnectionContext databaseContext, TapListConsumer<TapTableOptions> tapReadOffsetConsumer) {

  }

  private long batchCount(TapConnectorContext connectorContext, Object offset) {
    return 0;
  }

  private void batchRead(TapConnectorContext connectorContext, Object offset, TapReadOffsetConsumer<TapEvent> tapEventTapReadOffsetConsumer) {

  }

  private void streamRead(TapConnectorContext connectorContext, Object offset, TapReadOffsetConsumer<TapEvent> tapEventTapReadOffsetConsumer) {

  }

  private void handleDML(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapWriteListConsumer<TapRecordEvent> consumer) {

  }

  @Override
  public void sourceFunctions(SourceFunctions sourceFunctions) {
      sourceFunctions.withBatchCountFunction(this::batchCount);
      sourceFunctions.withBatchReadFunction(this::batchRead);
      sourceFunctions.withStreamReadFunction(this::streamRead);
  }

  @Override
  public void targetFunctions(TargetFunctions targetFunctions, SupportedTapEvents supportedTapEvents) {
      supportedTapEvents
          .supportDMLTypes(Arrays.asList(TapRecordEvent.TYPE_INSERT))
          .notSupportSchemaTypes()
          .notSupportTableTypes();
      targetFunctions.withDMLFunction(this::handleDML);
  }

  @Override
  public void close() {

  }
}
