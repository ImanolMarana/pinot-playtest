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
package org.apache.pinot.core.operator.query;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.TableResizer;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils.AggregationInfo;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.DefaultGroupByExecutor;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.startree.executor.StarTreeGroupByExecutor;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.spi.trace.Tracing;


/**
 * The <code>FilteredGroupByOperator</code> class provides the operator for group-by query on a single segment when
 * there are 1 or more filter expressions on aggregations.
 */
@SuppressWarnings("rawtypes")
public class FilteredGroupByOperator extends BaseOperator<GroupByResultsBlock> {
  private static final String EXPLAIN_NAME = "GROUP_BY_FILTERED";

  private final QueryContext _queryContext;
  private final AggregationFunction[] _aggregationFunctions;
  private final ExpressionContext[] _groupByExpressions;
  private final List<AggregationInfo> _aggregationInfos;
  private final long _numTotalDocs;
  private final DataSchema _dataSchema;

  private long _numDocsScanned;
  private long _numEntriesScannedInFilter;
  private long _numEntriesScannedPostFilter;

  public FilteredGroupByOperator(QueryContext queryContext, List<AggregationInfo> aggregationInfos, long numTotalDocs) {
    assert queryContext.getAggregationFunctions() != null && queryContext.getFilteredAggregationFunctions() != null
        && queryContext.getGroupByExpressions() != null;
    _queryContext = queryContext;
    _aggregationFunctions = queryContext.getAggregationFunctions();
    _groupByExpressions = queryContext.getGroupByExpressions().toArray(new ExpressionContext[0]);
    _aggregationInfos = aggregationInfos;
    _numTotalDocs = numTotalDocs;

    // NOTE: The indexedTable expects that the data schema will have group by columns before aggregation columns
    int numGroupByExpressions = _groupByExpressions.length;
    int numAggregationFunctions = _aggregationFunctions.length;
    int numColumns = numGroupByExpressions + numAggregationFunctions;
    String[] columnNames = new String[numColumns];
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[numColumns];

    // Extract column names and data types for group-by columns
    BaseProjectOperator<?> projectOperator = aggregationInfos.get(0).getProjectOperator();
    for (int i = 0; i < numGroupByExpressions; i++) {
      ExpressionContext groupByExpression = _groupByExpressions[i];
      columnNames[i] = groupByExpression.toString();
      columnDataTypes[i] = DataSchema.ColumnDataType.fromDataTypeSV(
          projectOperator.getResultColumnContext(groupByExpression).getDataType());
    }

    // Extract column names and data types for aggregation functions
    for (int i = 0; i < numAggregationFunctions; i++) {
      int index = numGroupByExpressions + i;
      Pair<AggregationFunction, FilterContext> pair = queryContext.getFilteredAggregationFunctions().get(i);
      AggregationFunction aggregationFunction = pair.getLeft();
      String columnName = AggregationFunctionUtils.getResultColumnName(aggregationFunction, pair.getRight());
      columnNames[index] = columnName;
      columnDataTypes[index] = aggregationFunction.getIntermediateResultColumnType();
    }

    _dataSchema = new DataSchema(columnNames, columnDataTypes);
  }

  @Override
  protected GroupByResultsBlock getNextBlock() {
    int numAggregations = _aggregationFunctions.length;
    GroupByResultHolder[] groupByResultHolders = new GroupByResultHolder[numAggregations];
    IdentityHashMap<AggregationFunction, Integer> resultHolderIndexMap =
        new IdentityHashMap<>(_aggregationFunctions.length);
    for (int i = 0; i < numAggregations; i++) {
      resultHolderIndexMap.put(_aggregationFunctions[i], i);
    }

    GroupKeyGenerator groupKeyGenerator = null;
    for (AggregationInfo aggregationInfo : _aggregationInfos) {
      groupKeyGenerator = processAggregationInfo(aggregationInfo, groupKeyGenerator, groupByResultHolders,
          resultHolderIndexMap);
    }

    assert groupKeyGenerator != null;
    for (GroupByResultHolder groupByResultHolder : groupByResultHolders) {
      groupByResultHolder.ensureCapacity(groupKeyGenerator.getNumKeys());
    }

    return generateGroupByResultsBlock(groupKeyGenerator, groupByResultHolders);
  }

  private GroupKeyGenerator processAggregationInfo(AggregationInfo aggregationInfo,
      GroupKeyGenerator groupKeyGenerator, GroupByResultHolder[] groupByResultHolders,
      IdentityHashMap<AggregationFunction, Integer> resultHolderIndexMap) {
    AggregationFunction[] aggregationFunctions = aggregationInfo.getFunctions();
    BaseProjectOperator<?> projectOperator = aggregationInfo.getProjectOperator();

    DefaultGroupByExecutor groupByExecutor = createGroupByExecutor(aggregationInfo, groupKeyGenerator,
        aggregationFunctions, projectOperator);
    groupKeyGenerator = groupByExecutor.getGroupKeyGenerator();

    int numDocsScanned = 0;
    ValueBlock valueBlock;
    while ((valueBlock = projectOperator.nextBlock()) != null) {
      numDocsScanned += valueBlock.getNumDocs();
      groupByExecutor.process(valueBlock);
    }

    _numDocsScanned += numDocsScanned;
    _numEntriesScannedInFilter += projectOperator.getExecutionStatistics().getNumEntriesScannedInFilter();
    _numEntriesScannedPostFilter += (long) numDocsScanned * projectOperator.getNumColumnsProjected();
    GroupByResultHolder[] filterGroupByResults = groupByExecutor.getGroupByResultHolders();
    for (int i = 0; i < aggregationFunctions.length; i++) {
      groupByResultHolders[resultHolderIndexMap.get(aggregationFunctions[i])] = filterGroupByResults[i];
    }
    return groupKeyGenerator;
  }

  private DefaultGroupByExecutor createGroupByExecutor(AggregationInfo aggregationInfo,
      GroupKeyGenerator groupKeyGenerator, AggregationFunction[] aggregationFunctions,
      BaseProjectOperator<?> projectOperator) {
    if (groupKeyGenerator == null) {
      if (aggregationInfo.isUseStarTree()) {
        return new StarTreeGroupByExecutor(_queryContext, aggregationFunctions, _groupByExpressions,
            projectOperator);
      } else {
        return new DefaultGroupByExecutor(_queryContext, aggregationFunctions, _groupByExpressions,
            projectOperator);
      }
    } else {
      if (aggregationInfo.isUseStarTree()) {
        return new StarTreeGroupByExecutor(_queryContext, aggregationFunctions, _groupByExpressions,
            projectOperator, groupKeyGenerator);
      } else {
        return new DefaultGroupByExecutor(_queryContext, aggregationFunctions, _groupByExpressions,
            projectOperator, groupKeyGenerator);
      }
    }
  }

  private GroupByResultsBlock generateGroupByResultsBlock(GroupKeyGenerator groupKeyGenerator,
      GroupByResultHolder[] groupByResultHolders) {
    // Check if the groups limit is reached
    boolean numGroupsLimitReached = groupKeyGenerator.getNumKeys() >= _queryContext.getNumGroupsLimit();
    Tracing.activeRecording().setNumGroups(_queryContext.getNumGroupsLimit(), groupKeyGenerator.getNumKeys());

    // Trim the groups when iff:
    // - Query has ORDER BY clause
    // - Segment group trim is enabled
    // - There are more groups than the trim size
    // TODO: Currently the groups are not trimmed if there is no ordering specified. Consider ordering on group-by
    //       columns if no ordering is specified.
    int minGroupTrimSize = _queryContext.getMinSegmentGroupTrimSize();
    if (_queryContext.getOrderByExpressions() != null && minGroupTrimSize > 0) {
      int trimSize = GroupByUtils.getTableCapacity(_queryContext.getLimit(), minGroupTrimSize);
      if (groupKeyGenerator.getNumKeys() > trimSize) {
        TableResizer tableResizer = new TableResizer(_dataSchema, _queryContext);
        Collection<IntermediateRecord> intermediateRecords =
            tableResizer.trimInSegmentResults(groupKeyGenerator, groupByResultHolders, trimSize);
        GroupByResultsBlock resultsBlock = new GroupByResultsBlock(_dataSchema, intermediateRecords, _queryContext);
        resultsBlock.setNumGroupsLimitReached(numGroupsLimitReached);
        return resultsBlock;
      }
    }

    AggregationGroupByResult aggGroupByResult =
        new AggregationGroupByResult(groupKeyGenerator, _aggregationFunctions, groupByResultHolders);
    GroupByResultsBlock resultsBlock = new GroupByResultsBlock(_dataSchema, aggGroupByResult, _queryContext);
    resultsBlock.setNumGroupsLimitReached(numGroupsLimitReached);
    return resultsBlock;
  }
//Refactoring end
  }

  @Override
  public List<Operator> getChildOperators() {
    return _aggregationInfos.stream().map(AggregationInfo::getProjectOperator).collect(Collectors.toList());
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return new ExecutionStatistics(_numDocsScanned, _numEntriesScannedInFilter, _numEntriesScannedPostFilter,
        _numTotalDocs);
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(groupKeys:");
    if (_groupByExpressions.length > 0) {
      stringBuilder.append(_groupByExpressions[0].toString());
      for (int i = 1; i < _groupByExpressions.length; i++) {
        stringBuilder.append(", ").append(_groupByExpressions[i].toString());
      }
    }

    stringBuilder.append(", aggregations:");
    if (_aggregationFunctions.length > 0) {
      stringBuilder.append(_aggregationFunctions[0].toExplainString());
      for (int i = 1; i < _aggregationFunctions.length; i++) {
        stringBuilder.append(", ").append(_aggregationFunctions[i].toExplainString());
      }
    }

    return stringBuilder.append(')').toString();
  }
}
