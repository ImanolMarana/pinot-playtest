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
package org.apache.pinot.core.query.request.context.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


public class QueryContextConverterUtils {
  private QueryContextConverterUtils() {
  }

  /**
   * Converts the given query into a {@link QueryContext}.
   */
  public static QueryContext getQueryContext(String query) {
    return getQueryContext(CalciteSqlParser.compileToPinotQuery(query));
  }

  /**
   * Converts the given {@link PinotQuery} into a {@link QueryContext}.
   */
  public static QueryContext getQueryContext(PinotQuery pinotQuery) {
    // FROM
    String tableName = getTableName(pinotQuery);
    QueryContext subquery = getSubquery(pinotQuery);

    // SELECT
    SelectComponents selectComponents = extractSelectComponents(pinotQuery);

    // WHERE
    FilterContext filter = getFilter(pinotQuery);

    // GROUP BY
    List<ExpressionContext> groupByExpressions = getGroupBys(pinotQuery);

    // ORDER BY
    List<OrderByExpressionContext> orderByExpressions = getOrderByExpressions(pinotQuery);

    // HAVING
    FilterContext havingFilter = getHavingFilter(pinotQuery);

    // EXPRESSION OVERRIDE HINTS
    Map<ExpressionContext, ExpressionContext> expressionContextOverrideHints =
        getExpressionOverrideHints(pinotQuery);

    return new QueryContext.Builder().setTableName(tableName).setSubquery(subquery)
        .setSelectExpressions(selectComponents.selectExpressions).setDistinct(selectComponents.distinct)
        .setAliasList(selectComponents.aliasList).setFilter(filter).setGroupByExpressions(groupByExpressions)
        .setOrderByExpressions(orderByExpressions).setHavingFilter(havingFilter)
        .setLimit(pinotQuery.getLimit()).setOffset(pinotQuery.getOffset())
        .setQueryOptions(pinotQuery.getQueryOptions())
        .setExpressionOverrideHints(expressionContextOverrideHints).setExplain(pinotQuery.isExplain())
        .build();
  }

  private static String getTableName(PinotQuery pinotQuery) {
    return pinotQuery.getDataSource().getTableName();
  }

  private static QueryContext getSubquery(PinotQuery pinotQuery) {
    DataSource dataSource = pinotQuery.getDataSource();
    if (dataSource.getSubquery() != null) {
      return getQueryContext(dataSource.getSubquery());
    }
    return null;
  }

  private static SelectComponents extractSelectComponents(PinotQuery pinotQuery) {
    List<Expression> selectList = pinotQuery.getSelectList();
    boolean distinct = false;
    // Handle DISTINCT
    if (selectList.size() == 1) {
      Function function = selectList.get(0).getFunctionCall();
      if (function != null && function.getOperator().equals("distinct")) {
        distinct = true;
        selectList = function.getOperands();
      }
    }

    List<String> aliasList = new ArrayList<>(selectList.size());
    List<ExpressionContext> selectExpressions = new ArrayList<>(selectList.size());
    for (Expression thriftExpression : selectList) {
      // Handle alias
      Function function = thriftExpression.getFunctionCall();
      Expression expressionWithoutAlias;
      if (function != null && function.getOperator().equals("as")) {
        List<Expression> operands = function.getOperands();
        expressionWithoutAlias = operands.get(0);
        aliasList.add(operands.get(1).getIdentifier().getName());
      } else {
        expressionWithoutAlias = thriftExpression;
        // Add null as a placeholder for alias
        aliasList.add(null);
      }
      selectExpressions.add(RequestContextUtils.getExpression(expressionWithoutAlias));
    }
    return new SelectComponents(selectExpressions, distinct, aliasList);
  }

  private static class SelectComponents {
    List<ExpressionContext> selectExpressions;
    boolean distinct;
    List<String> aliasList;

    public SelectComponents(List<ExpressionContext> selectExpressions, boolean distinct,
        List<String> aliasList) {
      this.selectExpressions = selectExpressions;
      this.distinct = distinct;
      this.aliasList = aliasList;
    }
  }

  private static FilterContext getFilter(PinotQuery pinotQuery) {
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      FilterContext filter = RequestContextUtils.getFilter(filterExpression);
      // Remove the filter if it is always true
      if (filter.isConstantTrue()) {
        return null;
      }
      return filter;
    }
    return null;
  }

  private static List<ExpressionContext> getGroupBys(PinotQuery pinotQuery) {
    List<Expression> groupByList = pinotQuery.getGroupByList();
    if (CollectionUtils.isNotEmpty(groupByList)) {
      List<ExpressionContext> groupByExpressions = new ArrayList<>(groupByList.size());
      for (Expression thriftExpression : groupByList) {
        groupByExpressions.add(RequestContextUtils.getExpression(thriftExpression));
      }
      return groupByExpressions;
    }
    return null;
  }

  private static List<OrderByExpressionContext> getOrderByExpressions(PinotQuery pinotQuery) {
    List<Expression> orderByList = pinotQuery.getOrderByList();
    if (CollectionUtils.isNotEmpty(orderByList)) {
      List<OrderByExpressionContext> orderByExpressions = new ArrayList<>(orderByList.size());
      Set<Expression> seen = new HashSet<>();
      for (Expression orderBy : orderByList) {
        Boolean isNullsLast = CalciteSqlParser.isNullsLast(orderBy);
        boolean isAsc = CalciteSqlParser.isAsc(orderBy, isNullsLast);
        Expression orderByFunctionsRemoved = CalciteSqlParser.removeOrderByFunctions(orderBy);
        // Deduplicate the order-by expressions
        if (seen.add(orderByFunctionsRemoved)) {
          ExpressionContext expressionContext =
              RequestContextUtils.getExpression(orderByFunctionsRemoved);
          if (isNullsLast != null) {
            orderByExpressions
                .add(new OrderByExpressionContext(expressionContext, isAsc, isNullsLast));
          } else {
            orderByExpressions.add(new OrderByExpressionContext(expressionContext, isAsc));
          }
        }
      }
      return orderByExpressions;
    }
    return null;
  }

  private static FilterContext getHavingFilter(PinotQuery pinotQuery) {
    Expression havingExpression = pinotQuery.getHavingExpression();
    if (havingExpression != null) {
      FilterContext havingFilter = RequestContextUtils.getFilter(havingExpression);
      // Remove the filter if it is always true
      if (havingFilter.isConstantTrue()) {
        return null;
      }
      return havingFilter;
    }
    return null;
  }

  private static Map<ExpressionContext, ExpressionContext> getExpressionOverrideHints(
      PinotQuery pinotQuery) {
    Map<ExpressionContext, ExpressionContext> expressionContextOverrideHints = new HashMap<>();
    Map<Expression, Expression> expressionOverrideHints = pinotQuery.getExpressionOverrideHints();
    if (expressionOverrideHints != null) {
      for (Map.Entry<Expression, Expression> entry : expressionOverrideHints.entrySet()) {
        expressionContextOverrideHints.put(RequestContextUtils.getExpression(entry.getKey()),
            RequestContextUtils.getExpression(entry.getValue()));
      }
    }
    return expressionContextOverrideHints;
  }

//Refactoring end