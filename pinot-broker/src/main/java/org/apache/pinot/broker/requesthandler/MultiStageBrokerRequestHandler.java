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
package org.apache.pinot.broker.requesthandler;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.querylog.QueryLogger;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.calcite.jdbc.CalciteSchemaBuilder;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerQueryPhase;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.ExceptionUtils;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.catalog.PinotCatalog;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.query.runtime.MultiStageStatsTreeBuilder;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.DatabaseConflictException;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MultiStageBrokerRequestHandler extends BaseBrokerRequestHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiStageBrokerRequestHandler.class);

  private final WorkerManager _workerManager;
  private final QueryDispatcher _queryDispatcher;

  public MultiStageBrokerRequestHandler(PinotConfiguration config, String brokerId, BrokerRoutingManager routingManager,
      AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager, TableCache tableCache) {
    super(config, brokerId, routingManager, accessControlFactory, queryQuotaManager, tableCache);
    String hostname = config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME);
    int port = Integer.parseInt(config.getProperty(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT));
    _workerManager = new WorkerManager(hostname, port, _routingManager);
    _queryDispatcher = new QueryDispatcher(new MailboxService(hostname, port, config));
    LOGGER.info("Initialized MultiStageBrokerRequestHandler on host: {}, port: {} with broker id: {}, timeout: {}ms, "
            + "query log max length: {}, query log max rate: {}", hostname, port, _brokerId, _brokerTimeoutMs,
        _queryLogger.getMaxQueryLengthToLog(), _queryLogger.getLogRateLimit());
  }

  @Override
  public void start() {
    _queryDispatcher.start();
  }

  @Override
  public void shutDown() {
    _queryDispatcher.shutdown();
  }

  @Override
  protected BrokerResponse handleRequest(long requestId, String query, @Nullable SqlNodeAndOptions sqlNodeAndOptions,
      JsonNode request, @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext,
      HttpHeaders httpHeaders, AccessControl accessControl) {
    LOGGER.debug("SQL query for request {}: {}", requestId, query);

    // Parse the query if needed
    sqlNodeAndOptions = parseQueryIfNeeded(requestId, query, sqlNodeAndOptions, request, requestContext);
    if (sqlNodeAndOptions == null) {
      return new BrokerResponseNative(QueryException.getException(QueryException.SQL_PARSING_ERROR,
          new RuntimeException("Failed to parse query: " + query)));
    }

    // Compile the request
    long compilationStartTimeNs = System.nanoTime();
    QueryCompilationResult compilationResult =
        compileQuery(requestId, sqlNodeAndOptions, httpHeaders, requestContext, requesterIdentity);
    if (compilationResult.brokerResponse != null) {
      return compilationResult.brokerResponse;
    }

    DispatchableSubPlan dispatchableSubPlan = compilationResult.dispatchableSubPlan;
    Set<String> tableNames = compilationResult.tableNames;
    long queryTimeoutMs = compilationResult.queryTimeoutMs;

    _brokerMetrics.addMeteredGlobalValue(BrokerMeter.MULTI_STAGE_QUERIES_GLOBAL, 1);
    for (String tableName : tableNames) {
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.MULTI_STAGE_QUERIES, 1);
    }

    requestContext.setTableNames(List.copyOf(tableNames));

    // Compilation Time. This includes the time taken for parsing, compiling, create stage plans and assigning workers.
    long compilationEndTimeNs = System.nanoTime();
    long compilationTimeNs =
        (compilationEndTimeNs - compilationStartTimeNs) + sqlNodeAndOptions.getParseTimeNs();
    updatePhaseTimingForTables(tableNames, BrokerQueryPhase.REQUEST_COMPILATION, compilationTimeNs);

    // Validate table access.
    if (!hasTableAccess(requesterIdentity, tableNames, requestContext, httpHeaders)) {
      throw new WebApplicationException("Permission denied", Response.Status.FORBIDDEN);
    }

    // Validate QPS quota
    if (hasExceededQPSQuota(tableNames, requestContext)) {
      String errorMessage = String.format("Request %d: %s exceeds query quota.", requestId, query);
      return new BrokerResponseNative(QueryException.getException(QueryException.QUOTA_EXCEEDED_ERROR, errorMessage));
    }

    long executionStartTimeNs = System.nanoTime();
    QueryDispatcher.QueryResult queryResults;
    try {
      queryResults =
          _queryDispatcher.submitAndReduce(requestContext, dispatchableSubPlan, queryTimeoutMs,
              compilationResult.queryOptions);
    } catch (TimeoutException e) {
      for (String table : tableNames) {
        _brokerMetrics.addMeteredTableValue(table, BrokerMeter.BROKER_RESPONSES_WITH_TIMEOUTS, 1);
      }
      LOGGER.warn("Timed out executing request {}: {}", requestId, query);
      requestContext.setErrorCode(QueryException.EXECUTION_TIMEOUT_ERROR_CODE);
      return new BrokerResponseNative(QueryException.EXECUTION_TIMEOUT_ERROR);
    } catch (Throwable t) {
      String consolidatedMessage = ExceptionUtils.consolidateExceptionMessages(t);
      LOGGER.error("Caught exception executing request {}: {}, {}", requestId, query, consolidatedMessage);
      requestContext.setErrorCode(QueryException.QUERY_EXECUTION_ERROR_CODE);
      return new BrokerResponseNative(
          QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, consolidatedMessage));
    }
    long executionEndTimeNs = System.nanoTime();
    updatePhaseTimingForTables(tableNames, BrokerQueryPhase.QUERY_EXECUTION,
        executionEndTimeNs - executionStartTimeNs);

    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
    brokerResponse.setResultTable(queryResults.getResultTable());
    // TODO: Add servers queried/responded stats
    brokerResponse.setBrokerReduceTimeMs(queryResults.getBrokerReduceTimeMs());

    // Attach unavailable segments
    int numUnavailableSegments = 0;
    for (Map.Entry<String, Set<String>> entry : dispatchableSubPlan.getTableToUnavailableSegmentsMap()
        .entrySet()) {
      String tableName = entry.getKey();
      Set<String> unavailableSegments = entry.getValue();
      numUnavailableSegments += unavailableSegments.size();
      brokerResponse.addException(QueryException.getException(QueryException.SERVER_SEGMENT_MISSING_ERROR,
          String.format("Find unavailable segments: %s for table: %s", unavailableSegments, tableName)));
    }
    requestContext.setNumUnavailableSegments(numUnavailableSegments);

    fillOldBrokerResponseStats(brokerResponse, queryResults.getQueryStats(), dispatchableSubPlan);

    // Set total query processing time
    // TODO: Currently we don't emit metric for QUERY_TOTAL_TIME_MS
    long totalTimeMs = System.currentTimeMillis() - requestContext.getRequestArrivalTimeMillis();
    brokerResponse.setTimeUsedMs(totalTimeMs);
    augmentStatistics(requestContext, brokerResponse);
    if (QueryOptionsUtils.shouldDropResults(compilationResult.queryOptions)) {
      brokerResponse.setResultTable(null);
    }

    // Log query and stats
    _queryLogger.log(
        new QueryLogger.QueryLogParams(requestContext, tableNames.toString(), brokerResponse, requesterIdentity,
            null));

    return brokerResponse;
  }

  private SqlNodeAndOptions parseQueryIfNeeded(long requestId, String query,
      @Nullable SqlNodeAndOptions sqlNodeAndOptions, JsonNode request, RequestContext requestContext) {
    if (sqlNodeAndOptions == null) {
      try {
        sqlNodeAndOptions = RequestUtils.parseQuery(query, request);
      } catch (Exception e) {
        // Do not log or emit metric here because it is pure user error
        requestContext.setErrorCode(QueryException.SQL_PARSING_ERROR_CODE);
        return null;
      }
    }
    return sqlNodeAndOptions;
  }

  private QueryCompilationResult compileQuery(long requestId, SqlNodeAndOptions sqlNodeAndOptions,
      HttpHeaders httpHeaders, RequestContext requestContext, RequesterIdentity requesterIdentity) {
    Map<String, String> queryOptions = sqlNodeAndOptions.getOptions();
    long queryTimeoutMs;
    QueryEnvironment.QueryPlannerResult queryPlanResult;
    Set<String> tableNames = null;
    DispatchableSubPlan dispatchableSubPlan = null;
    try {
      Long timeoutMsFromQueryOption = QueryOptionsUtils.getTimeoutMs(queryOptions);
      queryTimeoutMs = timeoutMsFromQueryOption != null ? timeoutMsFromQueryOption : _brokerTimeoutMs;
      String database = DatabaseUtils.extractDatabaseFromQueryRequest(queryOptions, httpHeaders);
      QueryEnvironment queryEnvironment = new QueryEnvironment(new TypeFactory(new TypeSystem()),
          CalciteSchemaBuilder.asRootSchema(new PinotCatalog(database, _tableCache), database), _workerManager,
          _tableCache);
      switch (sqlNodeAndOptions.getSqlNode().getKind()) {
        case EXPLAIN:
          queryPlanResult = queryEnvironment.explainQuery(sqlNodeAndOptions, requestId);
          String plan = queryPlanResult.getExplainPlan();
          tableNames = queryPlanResult.getTableNames();
          if (!hasTableAccess(requesterIdentity, tableNames, requestContext, httpHeaders)) {
            throw new WebApplicationException("Permission denied", Response.Status.FORBIDDEN);
          }
          return new QueryCompilationResult(queryOptions,
              constructMultistageExplainPlan(sqlNodeAndOptions.getSqlNode().toString(), plan));
        case SELECT:
        default:
          queryPlanResult = queryEnvironment.planQuery(sqlNodeAndOptions, requestId);
          dispatchableSubPlan = queryPlanResult.getQueryPlan();
          tableNames = queryPlanResult.getTableNames();
          break;
      }
    } catch (DatabaseConflictException e) {
      LOGGER.info("{}. Request {}: {}", e.getMessage(), requestId, sqlNodeAndOptions.getSqlNode());
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERY_VALIDATION_EXCEPTIONS, 1);
      requestContext.setErrorCode(QueryException.QUERY_VALIDATION_ERROR_CODE);
      return new QueryCompilationResult(queryOptions,
          new BrokerResponseNative(QueryException.getException(QueryException.QUERY_VALIDATION_ERROR, e)));
    } catch (WebApplicationException e) {
      throw e;
    } catch (RuntimeException e) {
      String consolidatedMessage = ExceptionUtils.consolidateExceptionMessages(e);
      LOGGER.warn("Caught exception planning request {}: {}, {}", requestId, sqlNodeAndOptions.getSqlNode(),
          consolidatedMessage);
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_COMPILATION_EXCEPTIONS, 1);
      if (e.getMessage().matches(".* Column .* not found in any table'")) {
        requestContext.setErrorCode(QueryException.UNKNOWN_COLUMN_ERROR_CODE);
        return new QueryCompilationResult(queryOptions,
            new BrokerResponseNative(
                QueryException.getException(QueryException.UNKNOWN_COLUMN_ERROR, consolidatedMessage)));
      }
      requestContext.setErrorCode(QueryException.QUERY_PLANNING_ERROR_CODE);
      return new QueryCompilationResult(queryOptions, new BrokerResponseNative(
          QueryException.getException(QueryException.QUERY_PLANNING_ERROR, consolidatedMessage)));
    }
    return new QueryCompilationResult(queryOptions, queryTimeoutMs, tableNames, dispatchableSubPlan);
  }

  private static class QueryCompilationResult {
    final Map<String, String> queryOptions;
    final long queryTimeoutMs;
    final Set<String> tableNames;
    final DispatchableSubPlan dispatchableSubPlan;
    final BrokerResponse brokerResponse;

    QueryCompilationResult(Map<String, String> queryOptions, long queryTimeoutMs, Set<String> tableNames,
        DispatchableSubPlan dispatchableSubPlan) {
      this.queryOptions = queryOptions;
      this.queryTimeoutMs = queryTimeoutMs;
      this.tableNames = tableNames;
      this.dispatchableSubPlan = dispatchableSubPlan;
      this.brokerResponse = null;
    }

    QueryCompilationResult(Map<String, String> queryOptions, BrokerResponse brokerResponse) {
      this.queryOptions = queryOptions;
      this.brokerResponse = brokerResponse;
      this.queryTimeoutMs = 0;
      this.tableNames = null;
      this.dispatchableSubPlan = null;
    }
  }

  //Rest of methods

//Refactoring end
  }

  private void fillOldBrokerResponseStats(BrokerResponseNativeV2 brokerResponse,
      List<MultiStageQueryStats.StageStats.Closed> queryStats, DispatchableSubPlan dispatchableSubPlan) {
    List<DispatchablePlanFragment> stagePlans = dispatchableSubPlan.getQueryStageList();
    List<PlanNode> planNodes = new ArrayList<>(stagePlans.size());
    for (DispatchablePlanFragment stagePlan : stagePlans) {
      planNodes.add(stagePlan.getPlanFragment().getFragmentRoot());
    }
    MultiStageStatsTreeBuilder treeBuilder = new MultiStageStatsTreeBuilder(planNodes, queryStats);
    brokerResponse.setStageStats(treeBuilder.jsonStatsByStage(0));
    for (MultiStageQueryStats.StageStats.Closed stageStats : queryStats) {
      if (stageStats != null) { // for example pipeline breaker may not have stats
        stageStats.forEach((type, stats) -> type.mergeInto(brokerResponse, stats));
      }
    }
  }

  /**
   * Validates whether the requester has access to all the tables.
   */
  private boolean hasTableAccess(RequesterIdentity requesterIdentity, Set<String> tableNames,
      RequestContext requestContext, HttpHeaders httpHeaders) {
    final long startTimeNs = System.nanoTime();
    AccessControl accessControl = _accessControlFactory.create();
    boolean hasAccess = accessControl.hasAccess(requesterIdentity, tableNames) && tableNames.stream()
        .allMatch(table -> accessControl.hasAccess(httpHeaders, TargetType.TABLE, table, Actions.Table.QUERY));
    if (!hasAccess) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.REQUEST_DROPPED_DUE_TO_ACCESS_ERROR, 1);
      LOGGER.warn("Access denied for requestId {}", requestContext.getRequestId());
      requestContext.setErrorCode(QueryException.ACCESS_DENIED_ERROR_CODE);
    }

    updatePhaseTimingForTables(tableNames, BrokerQueryPhase.AUTHORIZATION, System.nanoTime() - startTimeNs);

    return hasAccess;
  }

  /**
   * Returns true if the QPS quota of the tables has exceeded.
   */
  private boolean hasExceededQPSQuota(Set<String> tableNames, RequestContext requestContext) {
    for (String tableName : tableNames) {
      if (!_queryQuotaManager.acquire(tableName)) {
        LOGGER.warn("Request {}: query exceeds quota for table: {}", requestContext.getRequestId(), tableName);
        requestContext.setErrorCode(QueryException.TOO_MANY_REQUESTS_ERROR_CODE);
        String rawTableName = TableNameBuilder.extractRawTableName(tableName);
        _brokerMetrics.addMeteredTableValue(rawTableName, BrokerMeter.QUERY_QUOTA_EXCEEDED, 1);
        return true;
      }
    }
    return false;
  }

  private void updatePhaseTimingForTables(Set<String> tableNames, BrokerQueryPhase phase, long time) {
    for (String tableName : tableNames) {
      String rawTableName = TableNameBuilder.extractRawTableName(tableName);
      _brokerMetrics.addPhaseTiming(rawTableName, phase, time);
    }
  }

  private BrokerResponse constructMultistageExplainPlan(String sql, String plan) {
    BrokerResponseNative brokerResponse = BrokerResponseNative.empty();
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{sql, plan});
    DataSchema multistageExplainResultSchema = new DataSchema(new String[]{"SQL", "PLAN"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});
    brokerResponse.setResultTable(new ResultTable(multistageExplainResultSchema, rows));
    return brokerResponse;
  }

  @Override
  public Map<Long, String> getRunningQueries() {
    // TODO: Support running query tracking for multi-stage engine
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean cancelQuery(long queryId, int timeoutMs, Executor executor, HttpClientConnectionManager connMgr,
      Map<String, Integer> serverResponses) {
    // TODO: Support query cancellation for multi-stage engine
    throw new UnsupportedOperationException();
  }
}
