/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.reduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.util.GapfillUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>BrokerReduceService</code> class provides service to reduce data tables gathered from multiple servers
 * to {@link BrokerResponseNative}.
 */
@ThreadSafe
public class BrokerReduceService extends BaseReduceService {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerReduceService.class);

  public BrokerReduceService(PinotConfiguration config) {
    super(config);
  }

  public BrokerResponseNative reduceOnDataTable(BrokerRequest brokerRequest, BrokerRequest serverBrokerRequest,
      Map<ServerRoutingInstance, DataTable> dataTableMap, long reduceTimeOutMs, BrokerMetrics brokerMetrics) {
    if (dataTableMap.isEmpty()) {
      return BrokerResponseNative.empty();
    }

    BrokerResponseNative brokerResponseNative = new BrokerResponseNative();
    ExecutionStatsAggregator aggregator = initExecutionStatsAggregator(brokerRequest);
    DataSchema cachedDataSchema = processDataTableMetadata(dataTableMap, brokerResponseNative, aggregator,
        serverBrokerRequest, brokerMetrics);

    if (cachedDataSchema == null) {
      return brokerResponseNative;
    }

    reduceDataTables(brokerRequest, serverBrokerRequest, dataTableMap, reduceTimeOutMs, brokerMetrics,
        brokerResponseNative, cachedDataSchema);
    return brokerResponseNative;
  }
  
  private void reduceDataTables(BrokerRequest brokerRequest, BrokerRequest serverBrokerRequest,
      Map<ServerRoutingInstance, DataTable> dataTableMap, long reduceTimeOutMs, BrokerMetrics brokerMetrics,
      BrokerResponseNative brokerResponseNative, DataSchema cachedDataSchema) {
    QueryContext serverQueryContext = QueryContextConverterUtils.getQueryContext(serverBrokerRequest.getPinotQuery());
    DataTableReducer dataTableReducer = ResultReducerFactory.getResultReducer(serverQueryContext);

    Map<String, String> queryOptions = brokerRequest.getPinotQuery().getQueryOptions();
    int minGroupTrimSize = QueryOptionsUtils.getMinBrokerGroupTrimSizeOrDefault(queryOptions, _minGroupTrimSize);
    int groupTrimThreshold = QueryOptionsUtils.getGroupTrimThresholdOrDefault(queryOptions, _groupByTrimThreshold);

    String rawTableName = TableNameBuilder
        .extractRawTableName(serverBrokerRequest.getQuerySource().getTableName());
    try {
      dataTableReducer.reduceAndSetResults(rawTableName, cachedDataSchema, dataTableMap, brokerResponseNative,
          new DataTableReducerContext(_reduceExecutorService, _maxReduceThreadsPerQuery, reduceTimeOutMs,
              groupTrimThreshold, minGroupTrimSize), brokerMetrics);
    } catch (EarlyTerminationException e) {
      brokerResponseNative.addException(
          new QueryProcessingException(QueryException.QUERY_CANCELLATION_ERROR_CODE, e.toString()));
    }

    QueryContext queryContext =
        brokerRequest == serverBrokerRequest ? serverQueryContext
            : QueryContextConverterUtils.getQueryContext(brokerRequest.getPinotQuery());
    if (!serverQueryContext.isExplain()) {
      updateAlias(queryContext, brokerResponseNative);
    }
    processGapfill(serverQueryContext, brokerRequest, brokerResponseNative);
  }
  
  private void processGapfill(QueryContext serverQueryContext, BrokerRequest brokerRequest,
      BrokerResponseNative brokerResponseNative) {
    if (brokerRequest != serverQueryContext) {
      GapfillUtils.GapfillType gapfillType = GapfillUtils.getGapfillType(serverQueryContext);
      if (gapfillType == null) {
        throw new BadQueryRequestException("Nested query is not supported without gapfill");
      }
      BaseGapfillProcessor gapfillProcessor = GapfillProcessorFactory.getGapfillProcessor(serverQueryContext,
          gapfillType);
      gapfillProcessor.process(brokerResponseNative);
    }
  }

  private ExecutionStatsAggregator initExecutionStatsAggregator(BrokerRequest brokerRequest) {
    Map<String, String> queryOptions = brokerRequest.getPinotQuery().getQueryOptions();
    boolean enableTrace =
        queryOptions != null && Boolean.parseBoolean(queryOptions.get(CommonConstants.Broker.Request.TRACE));
    return new ExecutionStatsAggregator(enableTrace);
  }

  private DataSchema processDataTableMetadata(Map<ServerRoutingInstance, DataTable> dataTableMap,
      BrokerResponseNative brokerResponseNative, ExecutionStatsAggregator aggregator,
      BrokerRequest serverBrokerRequest, BrokerMetrics brokerMetrics) {
    DataSchema dataSchemaFromEmptyDataTable = null;
    DataSchema dataSchemaFromNonEmptyDataTable = null;
    List<ServerRoutingInstance> serversWithConflictingDataSchema = new ArrayList<>();

    Iterator<Map.Entry<ServerRoutingInstance, DataTable>> iterator = dataTableMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<ServerRoutingInstance, DataTable> entry = iterator.next();
      DataTable dataTable = entry.getValue();

      aggregator.aggregate(entry.getKey(), dataTable);

      DataSchema dataSchema = dataTable.getDataSchema();
      if (dataSchema == null) {
        iterator.remove();
      } else {
        if (dataTable.getNumberOfRows() == 0) {
          if (dataSchemaFromEmptyDataTable == null) {
            dataSchemaFromEmptyDataTable = dataSchema;
          }
          iterator.remove();
        } else {
          if (dataSchemaFromNonEmptyDataTable == null) {
            dataSchemaFromNonEmptyDataTable = dataSchema;
          } else {
            if (!Arrays.equals(dataSchema.getColumnDataTypes(),
                dataSchemaFromNonEmptyDataTable.getColumnDataTypes())) {
              serversWithConflictingDataSchema.add(entry.getKey());
              iterator.remove();
            }
          }
        }
      }
    }

    String tableName = serverBrokerRequest.getQuerySource().getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);

    aggregator.setStats(rawTableName, brokerResponseNative, brokerMetrics);

    reportConflictingDataSchema(serversWithConflictingDataSchema, tableName, rawTableName, brokerResponseNative,
        brokerMetrics);
    return dataSchemaFromNonEmptyDataTable != null ? dataSchemaFromNonEmptyDataTable : dataSchemaFromEmptyDataTable;
  }

  private void reportConflictingDataSchema(List<ServerRoutingInstance> serversWithConflictingDataSchema,
      String tableName, String rawTableName, BrokerResponseNative brokerResponseNative,
      BrokerMetrics brokerMetrics) {
    if (!serversWithConflictingDataSchema.isEmpty()) {
      String errorMessage =
          String.format("%s: responses for table: %s from servers: %s got dropped due to data schema inconsistency.",
              QueryException.MERGE_RESPONSE_ERROR.getMessage(), tableName, serversWithConflictingDataSchema);
      LOGGER.warn(errorMessage);
      brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.RESPONSE_MERGE_EXCEPTIONS, 1);
      brokerResponseNative.addException(
          new QueryProcessingException(QueryException.MERGE_RESPONSE_ERROR_CODE, errorMessage));
    }
  }

  public void shutDown() {
    _reduceExecutorService.shutdownNow();
  }

//Refactoring end
  }

  public void shutDown() {
    _reduceExecutorService.shutdownNow();
  }
}
