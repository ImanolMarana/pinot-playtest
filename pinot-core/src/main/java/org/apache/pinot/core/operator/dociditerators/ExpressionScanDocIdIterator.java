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
package org.apache.pinot.core.operator.dociditerators;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.BitmapDocIdSetOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.ProjectionOperatorUtils;
import org.apache.pinot.core.operator.blocks.DocIdSetBlock;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.BitmapDataProvider;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * The {@code ExpressionScanDocIdIterator} is the scan-based iterator for ExpressionFilterDocIdSet that can handle
 * filters on the expressions. It leverages the projection operator to batch processing the records block by block.
 */
public final class ExpressionScanDocIdIterator implements ScanBasedDocIdIterator {
  private final TransformFunction _transformFunction;
  private final PredicateEvaluator _predicateEvaluator;
  private final Map<String, DataSource> _dataSourceMap;
  private final int _endDocId;
  private final int[] _docIdBuffer = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
  private final boolean _nullHandlingEnabled;
  private final PredicateEvaluationResult _predicateEvaluationResult;

  private int _blockEndDocId = 0;
  private PeekableIntIterator _docIdIterator;

  // NOTE: Number of entries scanned is not accurate because we might need to scan multiple columns in order to solve
  //       the expression, but we only track the number of entries scanned for the resolved expression.
  private long _numEntriesScanned = 0L;

  public ExpressionScanDocIdIterator(TransformFunction transformFunction,
      @Nullable PredicateEvaluator predicateEvaluator, Map<String, DataSource> dataSourceMap, int numDocs,
      boolean nullHandlingEnabled, PredicateEvaluationResult predicateEvaluationResult) {
    _transformFunction = transformFunction;
    _predicateEvaluator = predicateEvaluator;
    _dataSourceMap = dataSourceMap;
    _endDocId = numDocs;
    _nullHandlingEnabled = nullHandlingEnabled;
    _predicateEvaluationResult = predicateEvaluationResult;
  }

  @Override
  public int next() {
    // If there are remaining records in the current block, return them first
    if (_docIdIterator != null && _docIdIterator.hasNext()) {
      return _docIdIterator.next();
    }

    // Evaluate the records in the next block
    while (_blockEndDocId < _endDocId) {
      int blockStartDocId = _blockEndDocId;
      _blockEndDocId = Math.min(blockStartDocId + DocIdSetPlanNode.MAX_DOC_PER_CALL, _endDocId);
      ProjectionBlock projectionBlock = ProjectionOperatorUtils.getProjectionOperator(_dataSourceMap,
          new RangeDocIdSetOperator(blockStartDocId, _blockEndDocId)).nextBlock();
      RoaringBitmap matchingDocIds = new RoaringBitmap();
      processProjectionBlock(projectionBlock, matchingDocIds);
      if (!matchingDocIds.isEmpty()) {
        _docIdIterator = matchingDocIds.getIntIterator();
        return _docIdIterator.next();
      }
    }

    return Constants.EOF;
  }

  @Override
  public int advance(int targetDocId) {
    if (targetDocId < _blockEndDocId) {
      // Search the current block first
      _docIdIterator.advanceIfNeeded(targetDocId);
      if (_docIdIterator.hasNext()) {
        return _docIdIterator.next();
      }
    } else {
      // Skip the blocks before the target document id
      _blockEndDocId = targetDocId;
    }

    // Search the block following the target document id
    _docIdIterator = null;
    return next();
  }

  @Override
  public MutableRoaringBitmap applyAnd(BatchIterator batchIterator, OptionalInt firstDoc, OptionalInt lastDoc) {
    IntIterator intIterator = batchIterator.asIntIterator(new int[OPTIMAL_ITERATOR_BATCH_SIZE]);
    ProjectionOperator projectionOperator = ProjectionOperatorUtils.getProjectionOperator(_dataSourceMap,
        new BitmapDocIdSetOperator(intIterator, _docIdBuffer));
    MutableRoaringBitmap matchingDocIds = new MutableRoaringBitmap();
    ProjectionBlock projectionBlock;
    while ((projectionBlock = projectionOperator.nextBlock()) != null) {
      processProjectionBlock(projectionBlock, matchingDocIds);
    }
    return matchingDocIds;
  }

  @Override
  public MutableRoaringBitmap applyAnd(ImmutableRoaringBitmap docIds) {
    ProjectionOperator projectionOperator =
        ProjectionOperatorUtils.getProjectionOperator(_dataSourceMap, new BitmapDocIdSetOperator(docIds, _docIdBuffer));
    MutableRoaringBitmap matchingDocIds = new MutableRoaringBitmap();
    ProjectionBlock projectionBlock;
    while ((projectionBlock = projectionOperator.nextBlock()) != null) {
      processProjectionBlock(projectionBlock, matchingDocIds);
    }
    return matchingDocIds;
  }

  private void processProjectionBlock(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds) {
    int numDocs = projectionBlock.getNumDocs();
    TransformResultMetadata resultMetadata = _transformFunction.getResultMetadata();
    if (resultMetadata.isSingleValue()) {
      processSingleValueProjectionBlock(projectionBlock, matchingDocIds, numDocs, resultMetadata);
    } else {
      processMultiValueProjectionBlock(projectionBlock, matchingDocIds, numDocs, resultMetadata);
    }
  }

  private void processSingleValueProjectionBlock(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds,
      int numDocs, TransformResultMetadata resultMetadata) {
    _numEntriesScanned += numDocs;
    if (_predicateEvaluationResult == PredicateEvaluationResult.NULL) {
      processNullPredicateEvaluationResult(projectionBlock, matchingDocIds);
      return;
    }
    boolean predicateEvaluationResult = _predicateEvaluationResult == PredicateEvaluationResult.TRUE;
    assert (_predicateEvaluator != null);
    if (resultMetadata.hasDictionary()) {
      processDictionaryEncodedSingleValue(projectionBlock, matchingDocIds, numDocs, predicateEvaluationResult);
    } else {
      processRawSingleValue(projectionBlock, matchingDocIds, numDocs, predicateEvaluationResult, resultMetadata);
    }
  }

  private void processMultiValueProjectionBlock(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds,
      int numDocs, TransformResultMetadata resultMetadata) {
    // TODO(https://github.com/apache/pinot/issues/10882): support NULL for multi-value.
    if (_predicateEvaluationResult == PredicateEvaluationResult.NULL) {
      return;
    }
    boolean predicateEvaluationResult = _predicateEvaluationResult == PredicateEvaluationResult.TRUE;
    assert (_predicateEvaluator != null);
    if (resultMetadata.hasDictionary()) {
      processDictionaryEncodedMultiValue(projectionBlock, matchingDocIds, numDocs, predicateEvaluationResult);
    } else {
      processRawMultiValue(projectionBlock, matchingDocIds, numDocs, predicateEvaluationResult, resultMetadata);
    }
  }

  private void processNullPredicateEvaluationResult(ProjectionBlock projectionBlock,
      BitmapDataProvider matchingDocIds) {
    RoaringBitmap nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
    if (nullBitmap != null) {
      for (int i : nullBitmap) {
        matchingDocIds.add(_docIdBuffer[i]);
      }
    }
  }

  private void processDictionaryEncodedSingleValue(ProjectionBlock projectionBlock,
      BitmapDataProvider matchingDocIds, int numDocs, boolean predicateEvaluationResult) {
    int[] dictIds = _transformFunction.transformToDictIdsSV(projectionBlock);
    RoaringBitmap nullBitmap = null;
    if (_nullHandlingEnabled) {
      nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
    }
    if (nullBitmap != null && !nullBitmap.isEmpty()) {
      for (int i = 0; i < numDocs; i++) {
        if (_predicateEvaluator.applySV(dictIds[i]) == predicateEvaluationResult && !nullBitmap.contains(i)) {
          matchingDocIds.add(_docIdBuffer[i]);
        }
      }
    } else {
      for (int i = 0; i < numDocs; i++) {
        if (_predicateEvaluator.applySV(dictIds[i]) == predicateEvaluationResult) {
          matchingDocIds.add(_docIdBuffer[i]);
        }
      }
    }
  }

  private void processRawSingleValue(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds, int numDocs,
      boolean predicateEvaluationResult, TransformResultMetadata resultMetadata) {
    switch (resultMetadata.getDataType().getStoredType()) {
      case INT:
        processIntValues(projectionBlock, matchingDocIds, numDocs, predicateEvaluationResult);
        break;
      case LONG:
        processLongValues(projectionBlock, matchingDocIds, numDocs, predicateEvaluationResult);
        break;
      case FLOAT:
        processFloatValues(projectionBlock, matchingDocIds, numDocs, predicateEvaluationResult);
        break;
      case DOUBLE:
        processDoubleValues(projectionBlock, matchingDocIds, numDocs, predicateEvaluationResult);
        break;
      case STRING:
        processStringValues(projectionBlock, matchingDocIds, numDocs, predicateEvaluationResult);
        break;
      case BYTES:
        processBytesValues(projectionBlock, matchingDocIds, numDocs, predicateEvaluationResult);
        break;
      case BIG_DECIMAL:
        processBigDecimalValues(projectionBlock, matchingDocIds, numDocs, predicateEvaluationResult);
        break;
      default:
        throw new IllegalStateException();
    }
  }

  private void processDictionaryEncodedMultiValue(ProjectionBlock projectionBlock,
      BitmapDataProvider matchingDocIds, int numDocs, boolean predicateEvaluationResult) {
    int[][] dictIdsArray = _transformFunction.transformToDictIdsMV(projectionBlock);
    for (int i = 0; i < numDocs; i++) {
      int[] dictIds = dictIdsArray[i];
      int numDictIds = dictIds.length;
      _numEntriesScanned += numDictIds;
      if (_predicateEvaluator.applyMV(dictIds, numDictIds) == predicateEvaluationResult) {
        matchingDocIds.add(_docIdBuffer[i]);
      }
    }
  }

  private void processRawMultiValue(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds, int numDocs,
      boolean predicateEvaluationResult, TransformResultMetadata resultMetadata) {
    switch (resultMetadata.getDataType().getStoredType()) {
      case INT:
        processIntValuesMV(projectionBlock, matchingDocIds, numDocs, predicateEvaluationResult);
        break;
      case LONG:
        processLongValuesMV(projectionBlock, matchingDocIds, numDocs, predicateEvaluationResult);
        break;
      case FLOAT:
        processFloatValuesMV(projectionBlock, matchingDocIds, numDocs, predicateEvaluationResult);
        break;
      case DOUBLE:
        processDoubleValuesMV(projectionBlock, matchingDocIds, numDocs, predicateEvaluationResult);
        break;
      case STRING:
        processStringValuesMV(projectionBlock, matchingDocIds, numDocs, predicateEvaluationResult);
        break;
      default:
        throw new IllegalStateException();
    }
  }

  private void processIntValues(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds, int numDocs,
      boolean predicateEvaluationResult) {
    int[] intValues = _transformFunction.transformToIntValuesSV(projectionBlock);
    RoaringBitmap nullBitmap = null;
    if (_nullHandlingEnabled) {
      nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
    }
    applyPredicate(matchingDocIds, numDocs, predicateEvaluationResult, intValues, nullBitmap);
  }

  private void processLongValues(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds, int numDocs,
      boolean predicateEvaluationResult) {
    long[] longValues = _transformFunction.transformToLongValuesSV(projectionBlock);
    RoaringBitmap nullBitmap = null;
    if (_nullHandlingEnabled) {
      nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
    }
    applyPredicate(matchingDocIds, numDocs, predicateEvaluationResult, longValues, nullBitmap);
  }

  private void processFloatValues(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds, int numDocs,
      boolean predicateEvaluationResult) {
    float[] floatValues = _transformFunction.transformToFloatValuesSV(projectionBlock);
    RoaringBitmap nullBitmap = null;
    if (_nullHandlingEnabled) {
      nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
    }
    applyPredicate(matchingDocIds, numDocs, predicateEvaluationResult, floatValues, nullBitmap);
  }

  private void processDoubleValues(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds, int numDocs,
      boolean predicateEvaluationResult) {
    double[] doubleValues = _transformFunction.transformToDoubleValuesSV(projectionBlock);
    RoaringBitmap nullBitmap = null;
    if (_nullHandlingEnabled) {
      nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
    }
    applyPredicate(matchingDocIds, numDocs, predicateEvaluationResult, doubleValues, nullBitmap);
  }

  private void processStringValues(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds, int numDocs,
      boolean predicateEvaluationResult) {
    String[] stringValues = _transformFunction.transformToStringValuesSV(projectionBlock);
    RoaringBitmap nullBitmap = null;
    if (_nullHandlingEnabled) {
      nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
    }
    applyPredicate(matchingDocIds, numDocs, predicateEvaluationResult, stringValues, nullBitmap);
  }

  private void processBytesValues(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds, int numDocs,
      boolean predicateEvaluationResult) {
    byte[][] bytesValues = _transformFunction.transformToBytesValuesSV(projectionBlock);
    RoaringBitmap nullBitmap = null;
    if (_nullHandlingEnabled) {
      nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
    }
    applyPredicate(matchingDocIds, numDocs, predicateEvaluationResult, bytesValues, nullBitmap);
  }

  private void processBigDecimalValues(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds,
      int numDocs, boolean predicateEvaluationResult) {
    BigDecimal[] bigDecimalValues = _transformFunction.transformToBigDecimalValuesSV(projectionBlock);
    RoaringBitmap nullBitmap = null;
    if (_nullHandlingEnabled) {
      nullBitmap = _transformFunction.getNullBitmap(projectionBlock);
    }
    applyPredicate(matchingDocIds, numDocs, predicateEvaluationResult, bigDecimalValues, nullBitmap);
  }

  private void processIntValuesMV(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds, int numDocs,
      boolean predicateEvaluationResult) {
    int[][] intValuesArray = _transformFunction.transformToIntValuesMV(projectionBlock);
    for (int i = 0; i < numDocs; i++) {
      int[] values = intValuesArray[i];
      int numValues = values.length;
      _numEntriesScanned += numValues;
      if (_predicateEvaluator.applyMV(values, numValues) == predicateEvaluationResult) {
        matchingDocIds.add(_docIdBuffer[i]);
      }
    }
  }

  private void processLongValuesMV(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds, int numDocs,
      boolean predicateEvaluationResult) {
    long[][] longValuesArray = _transformFunction.transformToLongValuesMV(projectionBlock);
    for (int i = 0; i < numDocs; i++) {
      long[] values = longValuesArray[i];
      int numValues = values.length;
      _numEntriesScanned += numValues;
      if (_predicateEvaluator.applyMV(values, numValues) == predicateEvaluationResult) {
        matchingDocIds.add(_docIdBuffer[i]);
      }
    }
  }

  private void processFloatValuesMV(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds, int numDocs,
      boolean predicateEvaluationResult) {
    float[][] floatValuesArray = _transformFunction.transformToFloatValuesMV(projectionBlock);
    for (int i = 0; i < numDocs; i++) {
      float[] values = floatValuesArray[i];
      int numValues = values.length;
      _numEntriesScanned += numValues;
      if (_predicateEvaluator.applyMV(values, numValues) == predicateEvaluationResult) {
        matchingDocIds.add(_docIdBuffer[i]);
      }
    }
  }

  private void processDoubleValuesMV(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds, int numDocs,
      boolean predicateEvaluationResult) {
    double[][] doubleValuesArray = _transformFunction.transformToDoubleValuesMV(projectionBlock);
    for (int i = 0; i < numDocs; i++) {
      double[] values = doubleValuesArray[i];
      int numValues = values.length;
      _numEntriesScanned += numValues;
      if (_predicateEvaluator.applyMV(values, numValues) == predicateEvaluationResult) {
        matchingDocIds.add(_docIdBuffer[i]);
      }
    }
  }

  private void processStringValuesMV(ProjectionBlock projectionBlock, BitmapDataProvider matchingDocIds, int numDocs,
      boolean predicateEvaluationResult) {
    String[][] valuesArray = _transformFunction.transformToStringValuesMV(projectionBlock);
    for (int i = 0; i < numDocs; i++) {
      String[] values = valuesArray[i];
      int numValues = values.length;
      _numEntriesScanned += numValues;
      if (_predicateEvaluator.applyMV(values, numValues) == predicateEvaluationResult) {
        matchingDocIds.add(_docIdBuffer[i]);
      }
    }
  }


  private void applyPredicate(BitmapDataProvider matchingDocIds, int numDocs, boolean predicateEvaluationResult,
      int[] values, RoaringBitmap nullBitmap) {
    if (nullBitmap != null && !nullBitmap.isEmpty()) {
      for (int i = 0; i < numDocs; i++) {
        if (_predicateEvaluator.applySV(values[i]) == predicateEvaluationResult && !nullBitmap.contains(i)) {
          matchingDocIds.add(_docIdBuffer[i]);
        }
      }
    } else {
      for (int i = 0; i < numDocs; i++) {
        if (_predicateEvaluator.applySV(values[i]) == predicateEvaluationResult) {
          matchingDocIds.add(_docIdBuffer[i]);
        }
      }
    }
  }

  private void applyPredicate(BitmapDataProvider matchingDocIds, int numDocs, boolean predicateEvaluationResult,
      long[] values, RoaringBitmap nullBitmap) {
    if (nullBitmap != null && !nullBitmap.isEmpty()) {
      for (int i = 0; i < numDocs; i++) {
        if (_predicateEvaluator.applySV(values[i]) == predicateEvaluationResult && !nullBitmap.contains(i)) {
          matchingDocIds.add(_docIdBuffer[i]);
        }
      }
    } else {
      for (int i = 0; i < numDocs; i++) {
        if (_predicateEvaluator.applySV(values[i]) == predicateEvaluationResult) {
          matchingDocIds.add(_docIdBuffer[i]);
        }
      }
    }
  }

  private void applyPredicate(BitmapDataProvider matchingDocIds, int numDocs, boolean predicateEvaluationResult,
      float[] values, RoaringBitmap nullBitmap) {
    if (nullBitmap != null && !nullBitmap.isEmpty()) {
      for (int i = 0; i < numDocs; i++) {
        if (_predicateEvaluator.applySV(values[i]) == predicateEvaluationResult && !nullBitmap.contains(i)) {
          matchingDocIds.add(_docIdBuffer[i]);
        }
      }
    } else {
      for (int i = 0; i < numDocs; i++) {
        if (_predicateEvaluator.applySV(values[i]) == predicateEvaluationResult) {
          matchingDocIds.add(_docIdBuffer[i]);
        }
      }
    }
  }

  private void applyPredicate(BitmapDataProvider matchingDocIds, int numDocs, boolean predicateEvaluationResult,
      double[] values, RoaringBitmap nullBitmap) {
    if (nullBitmap != null && !nullBitmap.isEmpty()) {
      for (int i = 0; i < numDocs; i++) {
        if (_predicateEvaluator.applySV(values[i]) == predicateEvaluationResult && !nullBitmap.contains(i)) {
          matchingDocIds.add(_docIdBuffer[i]);
        }
      }
    } else {
      for (int i = 0; i < numDocs; i++) {
        if (_predicateEvaluator.applySV(values[i]) == predicateEvaluationResult) {
          matchingDocIds.add(_docIdBuffer[i]);
        }
      }
    }
  }

  private void applyPredicate(BitmapDataProvider matchingDocIds, int numDocs, boolean predicateEvaluationResult,
      String[] values, RoaringBitmap nullBitmap) {
    if (nullBitmap != null && !nullBitmap.isEmpty()) {
      for (int i = 0; i < numDocs; i++) {
        if (_predicateEvaluator.applySV(values[i]) == predicateEvaluationResult && !nullBitmap.contains(i)) {
          matchingDocIds.add(_docIdBuffer[i]);
        }
      }
    } else {
      for (int i = 0; i < numDocs; i++) {
        if (_predicateEvaluator.applySV(values[i]) == predicateEvaluationResult) {
          matchingDocIds.add(_docIdBuffer[i]);
        }
      }
    }
  }

  private void applyPredicate(BitmapDataProvider matchingDocIds, int numDocs, boolean predicateEvaluationResult,
      byte[][] values, RoaringBitmap nullBitmap) {
    if (nullBitmap != null && !nullBitmap.isEmpty()) {
      for (int i = 0; i < numDocs; i++) {
        if (_predicateEvaluator.applySV(values[i]) == predicateEvaluationResult && !nullBitmap.contains(i)) {
          matchingDocIds.add(_docIdBuffer[i]);
        }
      }
    } else {
      for (int i = 0; i < numDocs; i++) {
        if (_predicateEvaluator.applySV(values[i]) == predicateEvaluationResult) {
          matchingDocIds.add(_docIdBuffer[i]);
        }
      }
    }
  }

  private void applyPredicate(BitmapDataProvider matchingDocIds, int numDocs, boolean predicateEvaluationResult,
      BigDecimal[] values, RoaringBitmap nullBitmap) {
    if (nullBitmap != null && !nullBitmap.isEmpty()) {
      for (int i = 0; i < numDocs; i++) {
        if (_predicateEvaluator.applySV(values[i]) == predicateEvaluationResult && !nullBitmap.contains(i)) {
          matchingDocIds.add(_docIdBuffer[i]);
        }
      }
    } else {
      for (int i = 0; i < numDocs; i++) {
        if (_predicateEvaluator.applySV(values[i]) == predicateEvaluationResult) {
          matchingDocIds.add(_docIdBuffer[i]);
        }
      }
    }
  }
//Refactoring end