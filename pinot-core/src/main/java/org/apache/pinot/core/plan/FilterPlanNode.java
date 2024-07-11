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
package org.apache.pinot.core.plan;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.predicate.JsonMatchPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.common.request.context.predicate.TextContainsPredicate;
import org.apache.pinot.common.request.context.predicate.TextMatchPredicate;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityPredicate;
import org.apache.pinot.core.geospatial.transform.function.StDistanceFunction;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.BitmapBasedFilterOperator;
import org.apache.pinot.core.operator.filter.EmptyFilterOperator;
import org.apache.pinot.core.operator.filter.ExpressionFilterOperator;
import org.apache.pinot.core.operator.filter.FilterOperatorUtils;
import org.apache.pinot.core.operator.filter.H3InclusionIndexFilterOperator;
import org.apache.pinot.core.operator.filter.H3IndexFilterOperator;
import org.apache.pinot.core.operator.filter.JsonMatchFilterOperator;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.operator.filter.TextContainsFilterOperator;
import org.apache.pinot.core.operator.filter.TextMatchFilterOperator;
import org.apache.pinot.core.operator.filter.VectorSimilarityFilterOperator;
import org.apache.pinot.core.operator.filter.predicate.FSTBasedRegexpPredicateEvaluatorFactory;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.NativeMutableTextIndex;
import org.apache.pinot.segment.local.segment.index.readers.text.NativeTextIndexReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class FilterPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final SegmentContext _segmentContext;
  private final QueryContext _queryContext;
  private final FilterContext _filter;

  // Cache the predicate evaluators
  private final List<Pair<Predicate, PredicateEvaluator>> _predicateEvaluators = new ArrayList<>(4);

  public FilterPlanNode(SegmentContext segmentContext, QueryContext queryContext) {
    this(segmentContext, queryContext, null);
  }

  public FilterPlanNode(SegmentContext segmentContext, QueryContext queryContext, @Nullable FilterContext filter) {
    _indexSegment = segmentContext.getIndexSegment();
    _segmentContext = segmentContext;
    _queryContext = queryContext;
    _filter = filter != null ? filter : _queryContext.getFilter();
  }

  @Override
  public BaseFilterOperator run() {
    MutableRoaringBitmap queryableDocIdsSnapshot = _segmentContext.getQueryableDocIdsSnapshot();
    int numDocs = _indexSegment.getSegmentMetadata().getTotalDocs();

    if (_filter != null) {
      BaseFilterOperator filterOperator = constructPhysicalOperator(_filter, numDocs);
      if (queryableDocIdsSnapshot != null) {
        BaseFilterOperator validDocFilter = new BitmapBasedFilterOperator(queryableDocIdsSnapshot, false, numDocs);
        return FilterOperatorUtils.getAndFilterOperator(_queryContext, Arrays.asList(filterOperator, validDocFilter),
            numDocs);
      } else {
        return filterOperator;
      }
    } else if (queryableDocIdsSnapshot != null) {
      return new BitmapBasedFilterOperator(queryableDocIdsSnapshot, false, numDocs);
    } else {
      return new MatchAllFilterOperator(numDocs);
    }
  }

  /**
   * Returns a mapping from predicates to their evaluators.
   */
  public List<Pair<Predicate, PredicateEvaluator>> getPredicateEvaluators() {
    return _predicateEvaluators;
  }

  /**
   * H3 index can be applied on ST_Distance iff:
   * <ul>
   *   <li>Predicate is of type RANGE</li>
   *   <li>Left-hand-side of the predicate is an ST_Distance function</li>
   *   <li>One argument of the ST_Distance function is an identifier, the other argument is an literal</li>
   *   <li>The identifier column has H3 index</li>
   * </ul>
   */
  private boolean canApplyH3IndexForDistanceCheck(Predicate predicate, FunctionContext function) {
    if (predicate.getType() != Predicate.Type.RANGE) {
      return false;
    }
    String functionName = function.getFunctionName();
    if (!functionName.equals("st_distance") && !functionName.equals("stdistance")) {
      return false;
    }
    List<ExpressionContext> arguments = function.getArguments();
    if (arguments.size() != 2) {
      throw new BadQueryRequestException("Expect 2 arguments for function: " + StDistanceFunction.FUNCTION_NAME);
    }
    // TODO: handle nested geography/geometry conversion functions
    String columnName = null;
    boolean findLiteral = false;
    for (ExpressionContext argument : arguments) {
      if (argument.getType() == ExpressionContext.Type.IDENTIFIER) {
        columnName = argument.getIdentifier();
      } else if (argument.getType() == ExpressionContext.Type.LITERAL) {
        findLiteral = true;
      }
    }
    return columnName != null && _indexSegment.getDataSource(columnName).getH3Index() != null && findLiteral
        && _queryContext.isIndexUseAllowed(columnName, FieldConfig.IndexType.H3);
  }

  /**
   * H3 index can be applied for inclusion check iff:
   * <ul>
   *   <li>Predicate is of type EQ</li>
   *   <li>Left-hand-side of the predicate is an ST_Within or ST_Contains function</li>
   *   <li>For ST_Within, the first argument is an identifier, the second argument is literal</li>
   *   <li>For ST_Contains function the first argument is literal, the second argument is an identifier</li>
   *   <li>The identifier column has H3 index</li>
   * </ul>
   */
  private boolean canApplyH3IndexForInclusionCheck(Predicate predicate, FunctionContext function) {
    if (predicate.getType() != Predicate.Type.EQ) {
      return false;
    }
    String functionName = function.getFunctionName();
    if (!functionName.equals("stwithin") && !functionName.equals("stcontains")) {
      return false;
    }
    List<ExpressionContext> arguments = function.getArguments();
    if (arguments.size() != 2) {
      throw new BadQueryRequestException("Expect 2 arguments for function: " + functionName);
    }
    // TODO: handle nested geography/geometry conversion functions
    if (functionName.equals("stwithin")) {
      if (arguments.get(0).getType() == ExpressionContext.Type.IDENTIFIER
          && arguments.get(1).getType() == ExpressionContext.Type.LITERAL) {
        String columnName = arguments.get(0).getIdentifier();
        return _indexSegment.getDataSource(columnName).getH3Index() != null
            && _queryContext.isIndexUseAllowed(columnName, FieldConfig.IndexType.H3);
      }
      return false;
    } else {
      if (arguments.get(1).getType() == ExpressionContext.Type.IDENTIFIER
          && arguments.get(0).getType() == ExpressionContext.Type.LITERAL) {
        String columnName = arguments.get(1).getIdentifier();
        return _indexSegment.getDataSource(columnName).getH3Index() != null
            && _queryContext.isIndexUseAllowed(columnName, FieldConfig.IndexType.H3);
      }
      return false;
    }
  }

  /**
   * Helper method to build the operator tree from the filter.
   */
  private BaseFilterOperator constructPhysicalOperator(FilterContext filter, int numDocs) {
    switch (filter.getType()) {
      case AND:
        return constructAndFilterOperator(filter, numDocs);
      case OR:
        return constructOrFilterOperator(filter, numDocs);
      case NOT:
        return constructNotFilterOperator(filter, numDocs);
      case PREDICATE:
        return constructPredicateFilterOperator(filter, numDocs);
      case CONSTANT:
        return filter.isConstantTrue() ? new MatchAllFilterOperator(numDocs) : EmptyFilterOperator.getInstance();
      default:
        throw new IllegalStateException();
    }
  }

  private BaseFilterOperator constructAndFilterOperator(FilterContext filter, int numDocs) {
    List<FilterContext> childFilters = filter.getChildren();
    List<BaseFilterOperator> childFilterOperators = new ArrayList<>(childFilters.size());
    for (FilterContext childFilter : childFilters) {
      BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter, numDocs);
      if (childFilterOperator.isResultEmpty()) {
        return EmptyFilterOperator.getInstance();
      } else if (!childFilterOperator.isResultMatchingAll()) {
        childFilterOperators.add(childFilterOperator);
      }
    }
    return FilterOperatorUtils.getAndFilterOperator(_queryContext, childFilterOperators, numDocs);
  }

  private BaseFilterOperator constructOrFilterOperator(FilterContext filter, int numDocs) {
    List<FilterContext> childFilters = filter.getChildren();
    List<BaseFilterOperator> childFilterOperators = new ArrayList<>(childFilters.size());
    for (FilterContext childFilter : childFilters) {
      BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter, numDocs);
      if (childFilterOperator.isResultMatchingAll()) {
        return new MatchAllFilterOperator(numDocs);
      } else if (!childFilterOperator.isResultEmpty()) {
        childFilterOperators.add(childFilterOperator);
      }
    }
    return FilterOperatorUtils.getOrFilterOperator(_queryContext, childFilterOperators, numDocs);
  }

  private BaseFilterOperator constructNotFilterOperator(FilterContext filter, int numDocs) {
    List<FilterContext> childFilters = filter.getChildren();
    assert childFilters.size() == 1;
    BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilters.get(0), numDocs);
    return FilterOperatorUtils.getNotFilterOperator(_queryContext, childFilterOperator, numDocs);
  }

  private BaseFilterOperator constructPredicateFilterOperator(FilterContext filter, int numDocs) {
    Predicate predicate = filter.getPredicate();
    ExpressionContext lhs = predicate.getLhs();
    if (lhs.getType() == ExpressionContext.Type.FUNCTION) {
      if (canApplyH3IndexForDistanceCheck(predicate, lhs.getFunction())) {
        return new H3IndexFilterOperator(_indexSegment, _queryContext, predicate, numDocs);
      } else if (canApplyH3IndexForInclusionCheck(predicate, lhs.getFunction())) {
        return new H3InclusionIndexFilterOperator(_indexSegment, _queryContext, predicate, numDocs);
      } else {
        // TODO: ExpressionFilterOperator does not support predicate types without PredicateEvaluator (TEXT_MATCH)
        return new ExpressionFilterOperator(_indexSegment, _queryContext, predicate, numDocs);
      }
    } else {
      return constructLeafFilterOperator(predicate, lhs.getIdentifier(), numDocs);
    }
  }

  private BaseFilterOperator constructLeafFilterOperator(Predicate predicate, String column, int numDocs) {
    DataSource dataSource = _indexSegment.getDataSource(column);
    switch (predicate.getType()) {
      case TEXT_CONTAINS:
        return constructTextContainsFilterOperator(dataSource, predicate, numDocs);
      case TEXT_MATCH:
        return constructTextMatchFilterOperator(dataSource, predicate, numDocs, column);
      case REGEXP_LIKE:
        return constructRegexpLikeFilterOperator(dataSource, predicate, numDocs);
      case JSON_MATCH:
        return constructJsonMatchFilterOperator(dataSource, predicate, numDocs, column);
      case VECTOR_SIMILARITY:
        return constructVectorSimilarityFilterOperator(dataSource, predicate, numDocs, column);
      case IS_NULL:
        return constructIsNullFilterOperator(dataSource, numDocs);
      case IS_NOT_NULL:
        return constructIsNotNullFilterOperator(dataSource, numDocs);
      default:
        PredicateEvaluator predicateEvaluator =
            PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dataSource, _queryContext);
        _predicateEvaluators.add(Pair.of(predicate, predicateEvaluator));
        return FilterOperatorUtils.getLeafFilterOperator(_queryContext, predicateEvaluator, dataSource,
            numDocs);
    }
  }

  private BaseFilterOperator constructTextContainsFilterOperator(DataSource dataSource, Predicate predicate,
      int numDocs) {
    TextIndexReader textIndexReader = dataSource.getTextIndex();
    if (!(textIndexReader instanceof NativeTextIndexReader)
        && !(textIndexReader instanceof NativeMutableTextIndex)) {
      throw new UnsupportedOperationException("TEXT_CONTAINS is supported only on native text index");
    }
    return new TextContainsFilterOperator(textIndexReader, (TextContainsPredicate) predicate, numDocs);
  }

  private BaseFilterOperator constructTextMatchFilterOperator(DataSource dataSource, Predicate predicate,
      int numDocs, String column) {
    TextIndexReader textIndexReader = dataSource.getTextIndex();
    Preconditions.checkState(textIndexReader != null,
        "Cannot apply TEXT_MATCH on column: %s without text index", column);
    // We could check for real time and segment Lucene reader, but easier to check the other way round
    if (textIndexReader instanceof NativeTextIndexReader
        || textIndexReader instanceof NativeMutableTextIndex) {
      throw new UnsupportedOperationException("TEXT_MATCH is not supported on native text index");
    }
    return new TextMatchFilterOperator(textIndexReader, (TextMatchPredicate) predicate, numDocs);
  }

  private BaseFilterOperator constructRegexpLikeFilterOperator(DataSource dataSource, Predicate predicate,
      int numDocs) {
    PredicateEvaluator predicateEvaluator;
    if (dataSource.getFSTIndex() != null) {
      predicateEvaluator =
          FSTBasedRegexpPredicateEvaluatorFactory.newFSTBasedEvaluator((RegexpLikePredicate) predicate,
              dataSource.getFSTIndex(), dataSource.getDictionary());
    } else {
      predicateEvaluator =
          PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dataSource.getDictionary(),
              dataSource.getDataSourceMetadata().getDataType());
    }
    _predicateEvaluators.add(Pair.of(predicate, predicateEvaluator));
    return FilterOperatorUtils.getLeafFilterOperator(_queryContext, predicateEvaluator, dataSource,
        numDocs);
  }

  private BaseFilterOperator constructJsonMatchFilterOperator(DataSource dataSource, Predicate predicate,
      int numDocs, String column) {
    JsonIndexReader jsonIndex = dataSource.getJsonIndex();
    Preconditions.checkState(jsonIndex != null, "Cannot apply JSON_MATCH on column: %s without json index",
        column);
    return new JsonMatchFilterOperator(jsonIndex, (JsonMatchPredicate) predicate, numDocs);
  }

  private BaseFilterOperator constructVectorSimilarityFilterOperator(DataSource dataSource,
      Predicate predicate, int numDocs, String column) {
    VectorIndexReader vectorIndex = dataSource.getVectorIndex();
    Preconditions.checkState(vectorIndex != null,
        "Cannot apply VECTOR_SIMILARITY on column: %s without vector index", column);
    return new VectorSimilarityFilterOperator(vectorIndex, (VectorSimilarityPredicate) predicate,
        numDocs);
  }

  private BaseFilterOperator constructIsNullFilterOperator(DataSource dataSource, int numDocs) {
    NullValueVectorReader nullValueVector = dataSource.getNullValueVector();
    if (nullValueVector != null) {
      return new BitmapBasedFilterOperator(nullValueVector.getNullBitmap(), false, numDocs);
    } else {
      return EmptyFilterOperator.getInstance();
    }
  }

  private BaseFilterOperator constructIsNotNullFilterOperator(DataSource dataSource, int numDocs) {
    NullValueVectorReader nullValueVector = dataSource.getNullValueVector();
    if (nullValueVector != null) {
      return new BitmapBasedFilterOperator(nullValueVector.getNullBitmap(), true, numDocs);
    } else {
      return new MatchAllFilterOperator(numDocs);
    }
  }
//Refactoring end
  }
}
